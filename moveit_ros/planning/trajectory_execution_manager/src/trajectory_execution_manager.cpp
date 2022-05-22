/*********************************************************************
 * Software License Agreement (BSD License)
 *
 *  Copyright (c) 2012, Willow Garage, Inc.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.
 *   * Neither the name of Willow Garage nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 *  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *********************************************************************/

/* Author: Ioan Sucan */

#include <moveit/trajectory_execution_manager/trajectory_execution_manager.h>
#include <moveit/robot_state/robot_state.h>
#include <geometric_shapes/check_isometry.h>
#include <moveit/robot_state/conversions.h>
#if __has_include(<tf2_eigen/tf2_eigen.hpp>)
#include <tf2_eigen/tf2_eigen.hpp>
#else
#include <tf2_eigen/tf2_eigen.h>
#endif

namespace trajectory_execution_manager
{
static const rclcpp::Logger LOGGER = rclcpp::get_logger("moveit_ros.trajectory_execution_manager");

const std::string TrajectoryExecutionManager::EXECUTION_EVENT_TOPIC = "trajectory_execution_event";

static const auto DEFAULT_CONTROLLER_INFORMATION_VALIDITY_AGE = rclcpp::Duration::from_seconds(1);
static const double DEFAULT_CONTROLLER_GOAL_DURATION_MARGIN = 0.5;  // allow 0.5s more than the expected execution time
                                                                    // before triggering a trajectory cancel (applied
                                                                    // after scaling)
static const double DEFAULT_CONTROLLER_GOAL_DURATION_SCALING =
    1.1;  // allow the execution of a trajectory to take more time than expected (scaled by a value > 1)

TrajectoryExecutionManager::TrajectoryExecutionManager(const rclcpp::Node::SharedPtr& node,
                                                       const moveit::core::RobotModelConstPtr& robot_model,
                                                       const planning_scene_monitor::PlanningSceneMonitorPtr& planning_scene_monitor)
  : node_(node), robot_model_(robot_model), planning_scene_monitor_(planning_scene_monitor), csm_(planning_scene_monitor_->getStateMonitor())
{
  if (!node_->get_parameter("moveit_manage_controllers", manage_controllers_))
    manage_controllers_ = false;
  initialize();
}

TrajectoryExecutionManager::TrajectoryExecutionManager(const rclcpp::Node::SharedPtr& node,
                                                       const moveit::core::RobotModelConstPtr& robot_model,
                                                       const planning_scene_monitor::PlanningSceneMonitorPtr& planning_scene_monitor,
                                                       bool manage_controllers)
  : node_(node), robot_model_(robot_model), planning_scene_monitor_(planning_scene_monitor), csm_(planning_scene_monitor_->getStateMonitor()), manage_controllers_(manage_controllers)
{
  initialize();
}

TrajectoryExecutionManager::~TrajectoryExecutionManager()
{
  run_continuous_execution_thread_ = false;
  stopBlockingExecution(true);
  private_executor_->cancel();
  if (private_executor_thread_.joinable())
    private_executor_thread_.join();
  private_executor_.reset();
}

void TrajectoryExecutionManager::initialize()
{
  verbose_ = false;
  execution_complete_ = true;
  stop_continuous_execution_ = false;
  current_context_ = -1;
  last_execution_status_blocking_ = moveit_controller_manager::ExecutionStatus::SUCCEEDED;
  run_continuous_execution_thread_ = true;
  execution_duration_monitoring_ = true;
  execution_velocity_scaling_ = 1.0;
  allowed_start_tolerance_ = 0.01;
  wait_for_trajectory_completion_ = true;

  allowed_execution_duration_scaling_ = DEFAULT_CONTROLLER_GOAL_DURATION_SCALING;
  allowed_goal_duration_margin_ = DEFAULT_CONTROLLER_GOAL_DURATION_MARGIN;

  // load controller-specific values for allowed_execution_duration_scaling and allowed_goal_duration_margin
  loadControllerParams();

  // load the controller manager plugin
  try
  {
    controller_manager_loader_ =
        std::make_unique<pluginlib::ClassLoader<moveit_controller_manager::MoveItControllerManager>>(
            "moveit_core", "moveit_controller_manager::MoveItControllerManager");
  }
  catch (pluginlib::PluginlibException& ex)
  {
    RCLCPP_FATAL_STREAM(LOGGER, "Exception while creating controller manager plugin loader: " << ex.what());
    return;
  }

  if (controller_manager_loader_)
  {
    std::string controller;

    if (!node_->get_parameter("moveit_controller_manager", controller))
    {
      const std::vector<std::string>& classes = controller_manager_loader_->getDeclaredClasses();
      if (classes.size() == 1)
      {
        controller = classes[0];
        RCLCPP_WARN(LOGGER,
                    "Parameter '~moveit_controller_manager' is not specified but only one "
                    "matching plugin was found: '%s'. Using that one.",
                    controller.c_str());
      }
      else
      {
        RCLCPP_FATAL(LOGGER, "Parameter '~moveit_controller_manager' not specified. This is needed to "
                             "identify the plugin to use for interacting with controllers. No paths can "
                             "be executed.");
      }
    }

    if (!controller.empty())
      try
      {
        // We make a node called moveit_simple_controller_manager so it's able to
        // receive callbacks on another thread. We then copy parameters from the move_group node
        // and then add it to the multithreadedexecutor
        rclcpp::NodeOptions opt;
        opt.allow_undeclared_parameters(true);
        opt.automatically_declare_parameters_from_overrides(true);
        controller_mgr_node_.reset(new rclcpp::Node("moveit_simple_controller_manager", opt));

        auto all_params = node_->get_node_parameters_interface()->get_parameter_overrides();
        for (const auto& param : all_params)
          controller_mgr_node_->set_parameter(rclcpp::Parameter(param.first, param.second));

        controller_manager_ = controller_manager_loader_->createUniqueInstance(controller);
        controller_manager_->initialize(controller_mgr_node_);
        private_executor_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
        private_executor_->add_node(controller_mgr_node_);

        // start executor on a different thread now
        private_executor_thread_ = std::thread([this]() { private_executor_->spin(); });
      }
      catch (pluginlib::PluginlibException& ex)
      {
        RCLCPP_FATAL_STREAM(LOGGER, "Exception while loading controller manager '" << controller << "': " << ex.what());
      }
  }

  // other configuration steps
  reloadControllerInformation();
  // The default callback group for rclcpp::Node is MutuallyExclusive which means we cannot call
  // receiveEvent while processing a different callback. To fix this we create a new callback group (the type is not
  // important since we only use it to process one callback) and associate event_topic_subscriber_ with this callback group
  auto callback_group = node_->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
  auto options = rclcpp::SubscriptionOptions();
  options.callback_group = callback_group;
  event_topic_subscriber_ = node_->create_subscription<std_msgs::msg::String>(
      EXECUTION_EVENT_TOPIC, 100, [this](const std_msgs::msg::String::SharedPtr event) { return receiveEvent(event); },
      options);

  controller_mgr_node_->get_parameter("trajectory_execution.execution_duration_monitoring",
                                  execution_duration_monitoring_);
  controller_mgr_node_->get_parameter("trajectory_execution.allowed_execution_duration_scaling",
                                      allowed_execution_duration_scaling_);
  controller_mgr_node_->get_parameter("trajectory_execution.allowed_goal_duration_margin",
                                      allowed_goal_duration_margin_);
  controller_mgr_node_->get_parameter("trajectory_execution.allowed_start_tolerance", allowed_start_tolerance_);

  if (manage_controllers_)
    RCLCPP_INFO(LOGGER, "Trajectory execution is managing controllers");
  else
    RCLCPP_INFO(LOGGER, "Trajectory execution is not managing controllers");

  auto controller_mgr_parameter_set_callback = [this](std::vector<rclcpp::Parameter> parameters) {
    auto result = rcl_interfaces::msg::SetParametersResult();
    result.successful = true;
    for (const auto& parameter : parameters)
    {
      const std::string& name = parameter.get_name();
      if (name == "trajectory_execution.execution_duration_monitoring")
        enableExecutionDurationMonitoring(parameter.as_bool());
      else if (name == "trajectory_execution.allowed_execution_duration_scaling")
        setAllowedExecutionDurationScaling(parameter.as_double());
      else if (name == "trajectory_execution.allowed_goal_duration_margin")
        setAllowedGoalDurationMargin(parameter.as_double());
      else if (name == "trajectory_execution.execution_velocity_scaling")
        setExecutionVelocityScaling(parameter.as_double());
      else if (name == "trajectory_execution.allowed_start_tolerance")
        setAllowedStartTolerance(parameter.as_double());
      else if (name == "trajectory_execution.wait_for_trajectory_completion")
        setWaitForTrajectoryCompletion(parameter.as_bool());
      else
        result.successful = false;
    }
    return result;
  };
  callback_handler_ = controller_mgr_node_->add_on_set_parameters_callback(controller_mgr_parameter_set_callback);
}

void TrajectoryExecutionManager::enableExecutionDurationMonitoring(bool flag)
{
  execution_duration_monitoring_ = flag;
}

void TrajectoryExecutionManager::setAllowedExecutionDurationScaling(double scaling)
{
  allowed_execution_duration_scaling_ = scaling;
}

void TrajectoryExecutionManager::setAllowedGoalDurationMargin(double margin)
{
  allowed_goal_duration_margin_ = margin;
}

void TrajectoryExecutionManager::setExecutionVelocityScaling(double scaling)
{
  execution_velocity_scaling_ = scaling;
}

void TrajectoryExecutionManager::setAllowedStartTolerance(double tolerance)
{
  allowed_start_tolerance_ = tolerance;
}

void TrajectoryExecutionManager::setWaitForTrajectoryCompletion(bool flag)
{
  wait_for_trajectory_completion_ = flag;
}

bool TrajectoryExecutionManager::isManagingControllers() const
{
  return manage_controllers_;
}

const moveit_controller_manager::MoveItControllerManagerPtr& TrajectoryExecutionManager::getControllerManager() const
{
  return controller_manager_;
}

void TrajectoryExecutionManager::processEvent(const std::string& event)
{
  if (event == "stop")
    stopBlockingExecution(true);
  else
    RCLCPP_WARN_STREAM(LOGGER, "Unknown event type: '" << event << "'");
}

void TrajectoryExecutionManager::receiveEvent(const std_msgs::msg::String::SharedPtr event)
{
  RCLCPP_DEBUG_STREAM(LOGGER, "Received event '" << event->data << "'");
  processEvent(event->data);
}

bool TrajectoryExecutionManager::pushToBlockingQueue(const moveit_msgs::msg::RobotTrajectory& trajectory, const std::string& controller)
{
  if (controller.empty())
    return pushToBlockingQueue(trajectory, std::vector<std::string>());
  else
    return pushToBlockingQueue(trajectory, std::vector<std::string>(1, controller));
}

bool TrajectoryExecutionManager::pushToBlockingQueue(const trajectory_msgs::msg::JointTrajectory& trajectory,
                                      const std::string& controller)
{
  if (controller.empty())
    return pushToBlockingQueue(trajectory, std::vector<std::string>());
  else
    return pushToBlockingQueue(trajectory, std::vector<std::string>(1, controller));
}

bool TrajectoryExecutionManager::pushToBlockingQueue(const trajectory_msgs::msg::JointTrajectory& trajectory,
                                      const std::vector<std::string>& controllers)
{
  moveit_msgs::msg::RobotTrajectory traj;
  traj.joint_trajectory = trajectory;
  return pushToBlockingQueue(traj, controllers);
}

bool TrajectoryExecutionManager::pushToBlockingQueue(const moveit_msgs::msg::RobotTrajectory& trajectory,
                                      const std::vector<std::string>& controllers)
{
  if (!execution_complete_)
  {
    RCLCPP_ERROR(LOGGER, "Cannot pushToBlockingQueue a new trajectory while another is being executed");
    return false;
  }
  TrajectoryExecutionContext* context = nullptr;
  if (configure(&context,trajectory, controllers))
  {
    context->blocking = true;
    if (verbose_)
    {
      std::stringstream ss;
      ss << "Pushed trajectory for execution using controllers [ ";
      for (const std::string& controller : context->controllers_)
        ss << controller << " ";
      ss << "]:" << '\n';
      // TODO: Provide message serialization
      // for (const moveit_msgs::msg::RobotTrajectory& trajectory_part : context->trajectory_parts_)
      // ss << trajectory_part << '\n';
      RCLCPP_DEBUG_STREAM(LOGGER, ss.str());
    }
    trajectories_.push_back(context);
    return true;
  }
  else
  {
    delete context;
    last_execution_status_blocking_ = moveit_controller_manager::ExecutionStatus::ABORTED;
  }

  return false;
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const moveit_msgs::msg::RobotTrajectory& trajectory,
                                                const std::string& controller, const ExecutionCompleteCallback& callback,
                                                const rclcpp::Duration& backlog_timeout)
{
  if (controller.empty())
    return pushAndExecuteSimultaneous(trajectory, std::vector<std::string>(), callback,backlog_timeout);
  else
    return pushAndExecuteSimultaneous(trajectory, std::vector<std::string>(1, controller), callback,backlog_timeout);
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const trajectory_msgs::msg::JointTrajectory& trajectory,
                                                const std::string& controller, const ExecutionCompleteCallback& callback,
                                                const rclcpp::Duration& backlog_timeout)
{
  if (controller.empty())
    return pushAndExecuteSimultaneous(trajectory, std::vector<std::string>(), callback,backlog_timeout);
  else
    return pushAndExecuteSimultaneous(trajectory, std::vector<std::string>(1, controller), callback,backlog_timeout);
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const sensor_msgs::msg::JointState& state, const std::string& controller, const ExecutionCompleteCallback& callback,
                                                            const rclcpp::Duration& backlog_timeout)
{
  if (controller.empty())
    return pushAndExecuteSimultaneous(state, std::vector<std::string>(), callback,backlog_timeout);
  else
    return pushAndExecuteSimultaneous(state, std::vector<std::string>(1, controller), callback,backlog_timeout);
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const trajectory_msgs::msg::JointTrajectory& trajectory,
                                                const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback,
                                                const rclcpp::Duration& backlog_timeout)
{
  moveit_msgs::msg::RobotTrajectory traj;
  traj.joint_trajectory = trajectory;
  return pushAndExecuteSimultaneous(traj, controllers, callback,backlog_timeout);
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const sensor_msgs::msg::JointState& state,
                                                const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback,
                                                const rclcpp::Duration& backlog_timeout)
{
  moveit_msgs::msg::RobotTrajectory traj;
  traj.joint_trajectory.header = state.header;
  traj.joint_trajectory.joint_names = state.name;
  traj.joint_trajectory.points.resize(1);
  traj.joint_trajectory.points[0].positions = state.position;
  traj.joint_trajectory.points[0].velocities = state.velocity;
  traj.joint_trajectory.points[0].effort = state.effort;
  traj.joint_trajectory.points[0].time_from_start = rclcpp::Duration(0, 0);
  return pushAndExecuteSimultaneous(traj, controllers, callback,backlog_timeout);
}

bool TrajectoryExecutionManager::pushAndExecuteSimultaneous(const moveit_msgs::msg::RobotTrajectory& trajectory,
                                                const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback,
                                                const rclcpp::Duration& backlog_timeout)
{
  TrajectoryExecutionContext* context = nullptr;
  if (configure(&context,trajectory, controllers))
  {
    context->execution_complete_callback = callback;
    context->backlog_timeout = backlog_timeout;
    context->blocking = false;
    RCLCPP_DEBUG(LOGGER, "Backlog_timeout: %f",backlog_timeout.seconds());
    {
      boost::mutex::scoped_lock slock(continuous_execution_thread_mutex_);
      RCLCPP_INFO(LOGGER,"Continuous execution thread locked");
      continuous_execution_queue_.push_back(context);
      if (!continuous_execution_thread_)
        continuous_execution_thread_.reset(
            new boost::thread(boost::bind(&TrajectoryExecutionManager::continuousExecutionThread, this)));
    }
    context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::SUCCEEDED;
    continuous_execution_condition_.notify_all();
    return true;
  }
  else
  {
    delete context;
    return false;
  }
}

void TrajectoryExecutionManager::continuousExecutionThread()
{
  rclcpp::Rate r(10);
  while(run_continuous_execution_thread_)
  {  
    RCLCPP_DEBUG(LOGGER,"Main Loop");
    if (!stop_continuous_execution_ && ( !active_contexts_.empty() || !backlog.empty()) && continuous_execution_queue_.empty())
    {
      if(!checkAllRemainingPaths()){
        RCLCPP_ERROR(LOGGER,"Path is not valid anymore");

      }
      RCLCPP_DEBUG(LOGGER,"Looping");
      checkBacklogExpiration();
      r.sleep();
    }
    // Now it always runs, repair this
    /*
    else if(active_contexts_.empty() && backlog.empty() && continuous_execution_queue_.empty()){
        boost::unique_lock<boost::mutex> ulock(continuous_execution_thread_mutex_);
        continuous_execution_condition_.wait(ulock);
    }
    */

    // If stop-flag is set, break out
    if (stop_continuous_execution_ || !run_continuous_execution_thread_)
    {
      RCLCPP_ERROR_STREAM(LOGGER, "Stop!. stop_continuous_execution: " << stop_continuous_execution_ << " run_continuous_execution_thread_: " << run_continuous_execution_thread_);
      // Cancel ongoing executions
      stopBlockingExecution(true);
      // Clear map and used handles set
      active_contexts_.clear();
      used_handles.clear();
      backlog.clear();
      while (!continuous_execution_queue_.empty() || !backlog.empty())
      {
        TrajectoryExecutionContext* context = continuous_execution_queue_.front();
        RCLCPP_DEBUG_STREAM(LOGGER, "Calling completed callback to abort");
        context->execution_complete_callback(moveit_controller_manager::ExecutionStatus::ABORTED);
        continuous_execution_queue_.pop_front();
        delete context;
      }
      stop_continuous_execution_ = false;
      continue;
    }

    while (!continuous_execution_queue_.empty())
    {
      // Get next trajectory context from queue
      RCLCPP_DEBUG(LOGGER, "Queue Size: %li",continuous_execution_queue_.size());
      TrajectoryExecutionContext* context = nullptr;
      {
        boost::mutex::scoped_lock slock(continuous_execution_thread_mutex_);
        if (continuous_execution_queue_.empty())
          break;
        context = continuous_execution_queue_.front();
        continuous_execution_queue_.pop_front();
        if (continuous_execution_queue_.empty())
          continuous_execution_condition_.notify_all();
      }

      RCLCPP_DEBUG(LOGGER, "Popped element with duration %lf from queue. Remaining length: %li", context->trajectory_.getDuration(),
                         continuous_execution_queue_.size());

      // First make sure desired controllers are active
      if (!areControllersActive(context->controllers_))
      {
        RCLCPP_ERROR(LOGGER, "Not all needed controllers are active. Cannot pushToBlockingQueue and execute. You can try "
                             "calling ensureActiveControllers() before pushAndExecuteSimultaneous()");
        context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::ABORTED;
        RCLCPP_INFO(LOGGER, "Calling completed callback");
        context->execution_complete_callback(moveit_controller_manager::ExecutionStatus::ABORTED);
        delete context;
        continue;
      }

      // Check that this context's controller handles are not used in the backlog. Otherwise, add to backlog (because trajectories need to be executed in order)
      bool controllers_not_used_in_backlog = true;
      for (auto backlog_context : backlog)
        if (hasCommonHandles(*backlog_context.first, *context))
        {
          RCLCPP_DEBUG(LOGGER, "Request with duration %lf", context->trajectory_.getDuration());
          RCLCPP_DEBUG(LOGGER, "has handles blocked by backlog items. push_back to backlog");
          backlog.push_back(std::pair<TrajectoryExecutionContext*, rclcpp::Time> (context, node_->now()));
          controllers_not_used_in_backlog = false;
          break;
        }

      if(controllers_not_used_in_backlog && !validateAndExecuteContext(*context))
      {
        RCLCPP_DEBUG(LOGGER, "Request: %lf not executable, pushing it into backlog", context->trajectory_.getDuration());
        backlog.push_back(std::pair<TrajectoryExecutionContext*, rclcpp::Time> (context, node_->now()));
      }
    }
  }
}
void TrajectoryExecutionManager::checkBacklogExpiration()
{
  for (auto it = backlog.begin(); it != backlog.end(); )
  {
    TrajectoryExecutionContext* current_context = it->first;
    rclcpp::Time& created_at = it->second;
    // Remove backlog items that have expired 
    if (created_at +  current_context->backlog_timeout < node_->now())
    {
      RCLCPP_WARN(LOGGER, "Backlog item with duration %f has expired (Timeout: %f). Removing from backlog.", current_context->trajectory_.getDuration(),current_context->backlog_timeout.seconds());
      current_context->execution_complete_callback(moveit_controller_manager::ExecutionStatus::TIMED_OUT);
      it = backlog.erase(it);
      continue;
    }else{
      it++;
    }
  }
}
void TrajectoryExecutionManager::checkBacklog()
{
  // Check all backlog entries for trajectories that can now be executed 
  for (auto it = backlog.begin(); it != backlog.end(); )
  {
    TrajectoryExecutionContext* current_context = it->first;
    
    // Validate that the handles used in this context are not already in earlier (= higher priority) backlogged trajectories
    bool controllers_not_used_earlier_in_backlog = true;
    RCLCPP_DEBUG(LOGGER, "Backlog evaluation of item: %f",current_context->trajectory_.getDuration());
    for (auto it2 = backlog.begin(); it2 != it; ++it2)
    {
      TrajectoryExecutionContext* priority_context = it2->first; // Previous context in the backlog (earlier ones have priority)
      RCLCPP_DEBUG(LOGGER, "Backlog comparing item with duration: %lf" ,current_context->trajectory_.getDuration());
      RCLCPP_DEBUG(LOGGER, "vs item with duration: %f", priority_context->trajectory_.getDuration());
      if (hasCommonHandles(*current_context, *priority_context))
      {
        controllers_not_used_earlier_in_backlog = false;
        RCLCPP_DEBUG(LOGGER, "Backlog item has handles blocked by previous items");
        break;
      }
    }
    if(controllers_not_used_earlier_in_backlog)
    {
      RCLCPP_DEBUG(LOGGER, "Backlog item with duration %lf will be checked and pushed to controller.", current_context->trajectory_.getDuration());
      if(validateAndExecuteContext(*current_context))
      {
        RCLCPP_DEBUG(LOGGER, "Backlog item with duration %lf has been executed correctly.", current_context->trajectory_.getDuration() );
        it = backlog.erase(it);
      }
      else if (it == backlog.begin() && active_contexts_.empty())
      {
        RCLCPP_ERROR_STREAM(LOGGER, "Trajectory is in a deadlock, aborting");
        // Since there is not active trajectory being executed but this Top priority backlog-trajectory is not executable, abort it.
        current_context->execution_complete_callback(moveit_controller_manager::ExecutionStatus::ABORTED);
        it = backlog.erase(it);
      }
      else
      {
        RCLCPP_DEBUG(LOGGER, "Backlog item with duration %lf is still not executable", current_context->trajectory_.getDuration());
        it++;
      }
    }
    else
      it++;
  }
  RCLCPP_DEBUG_STREAM(LOGGER, "Done checking backlog, size: " << backlog.size());
}

void TrajectoryExecutionManager::reloadControllerInformation()
{
  known_controllers_.clear();
  if (controller_manager_)
  {
    std::vector<std::string> names;
    controller_manager_->getControllersList(names);
    for (const std::string& name : names)
    {
      std::vector<std::string> joints;
      controller_manager_->getControllerJoints(name, joints);
      ControllerInformation ci;
      ci.name_ = name;
      ci.joints_.insert(joints.begin(), joints.end());
      known_controllers_[ci.name_] = ci;
    }

    names.clear();
    controller_manager_->getActiveControllers(names);
    for (const auto& active_name : names)
    {
      auto found_it = std::find_if(known_controllers_.begin(), known_controllers_.end(),
                                   [&](const auto& known_controller) { return known_controller.first == active_name; });
      if (found_it != known_controllers_.end())
      {
        found_it->second.state_.active_ = true;
      }
    }
    
    for (std::map<std::string, ControllerInformation>::iterator it = known_controllers_.begin();
         it != known_controllers_.end(); ++it)
      for (std::map<std::string, ControllerInformation>::iterator jt = known_controllers_.begin();
           jt != known_controllers_.end(); ++jt)
        if (it != jt)
        {
          std::vector<std::string> intersect;
          std::set_intersection(it->second.joints_.begin(), it->second.joints_.end(), jt->second.joints_.begin(),
                                jt->second.joints_.end(), std::back_inserter(intersect));
          if (!intersect.empty())
          {
            it->second.overlapping_controllers_.insert(jt->first);
            jt->second.overlapping_controllers_.insert(it->first);
          }
        }
  }
  else
  {
    RCLCPP_ERROR(LOGGER, "Failed to reload controllers: `controller_manager_` does not exist.");
  }
}

void TrajectoryExecutionManager::updateControllerState(const std::string& controller, const rclcpp::Duration& age)
{
  std::map<std::string, ControllerInformation>::iterator it = known_controllers_.find(controller);
  if (it != known_controllers_.end())
    updateControllerState(it->second, age);
  else
    RCLCPP_ERROR(LOGGER, "Controller '%s' is not known.", controller.c_str());
}

void TrajectoryExecutionManager::updateControllerState(ControllerInformation& ci, const rclcpp::Duration& age)
{
  if (node_->now() - ci.last_update_ >= age)
  {
    if (controller_manager_)
    {
      if (verbose_)
        RCLCPP_INFO(LOGGER, "Updating information for controller '%s'.", ci.name_.c_str());
      ci.state_ = controller_manager_->getControllerState(ci.name_);
      ci.last_update_ = node_->now();
    }
  }
  else if (verbose_)
    RCLCPP_INFO(LOGGER, "Information for controller '%s' is assumed to be up to date.", ci.name_.c_str());
}

void TrajectoryExecutionManager::updateControllersState(const rclcpp::Duration& age)
{
  for (std::pair<const std::string, ControllerInformation>& known_controller : known_controllers_)
    updateControllerState(known_controller.second, age);
}

bool TrajectoryExecutionManager::checkControllerCombination(std::vector<std::string>& selected,
                                                            const std::set<std::string>& actuated_joints)
{
  std::set<std::string> combined_joints;
  for (const std::string& controller : selected)
  {
    const ControllerInformation& ci = known_controllers_[controller];
    combined_joints.insert(ci.joints_.begin(), ci.joints_.end());
  }

  if (verbose_)
  {
    std::stringstream ss, saj, sac;
    for (const std::string& controller : selected)
      ss << controller << " ";
    for (const std::string& actuated_joint : actuated_joints)
      saj << actuated_joint << " ";
    for (const std::string& combined_joint : combined_joints)
      sac << combined_joint << " ";
    RCLCPP_INFO(LOGGER, "Checking if controllers [ %s] operating on joints [ %s] cover joints [ %s]", ss.str().c_str(),
                sac.str().c_str(), saj.str().c_str());
  }

  return std::includes(combined_joints.begin(), combined_joints.end(), actuated_joints.begin(), actuated_joints.end());
}

void TrajectoryExecutionManager::generateControllerCombination(std::size_t start_index, std::size_t controller_count,
                                                               const std::vector<std::string>& available_controllers,
                                                               std::vector<std::string>& selected_controllers,
                                                               std::vector<std::vector<std::string>>& selected_options,
                                                               const std::set<std::string>& actuated_joints)
{
  if (selected_controllers.size() == controller_count)
  {
    if (checkControllerCombination(selected_controllers, actuated_joints))
      selected_options.push_back(selected_controllers);
    return;
  }

  for (std::size_t i = start_index; i < available_controllers.size(); ++i)
  {
    bool overlap = false;
    const ControllerInformation& ci = known_controllers_[available_controllers[i]];
    for (std::size_t j = 0; j < selected_controllers.size() && !overlap; ++j)
    {
      if (ci.overlapping_controllers_.find(selected_controllers[j]) != ci.overlapping_controllers_.end())
        overlap = true;
    }
    if (overlap)
      continue;
    selected_controllers.push_back(available_controllers[i]);
    generateControllerCombination(i + 1, controller_count, available_controllers, selected_controllers,
                                  selected_options, actuated_joints);
    selected_controllers.pop_back();
  }
}

namespace
{
struct OrderPotentialControllerCombination
{
  bool operator()(const std::size_t a, const std::size_t b) const
  {
    // preference is given to controllers marked as default
    if (nrdefault[a] > nrdefault[b])
      return true;
    if (nrdefault[a] < nrdefault[b])
      return false;

    // and then to ones that operate on fewer joints
    if (nrjoints[a] < nrjoints[b])
      return true;
    if (nrjoints[a] > nrjoints[b])
      return false;

    // and then to active ones
    if (nractive[a] < nractive[b])
      return true;
    if (nractive[a] > nractive[b])
      return false;

    return false;
  }

  std::vector<std::vector<std::string>> selected_options;
  std::vector<std::size_t> nrdefault;
  std::vector<std::size_t> nrjoints;
  std::vector<std::size_t> nractive;
};
}  // namespace

bool TrajectoryExecutionManager::findControllers(const std::set<std::string>& actuated_joints,
                                                 std::size_t controller_count,
                                                 const std::vector<std::string>& available_controllers,
                                                 std::vector<std::string>& selected_controllers)
{
  // generate all combinations of controller_count controllers that operate on disjoint sets of joints
  std::vector<std::string> work_area;
  OrderPotentialControllerCombination order;
  std::vector<std::vector<std::string>>& selected_options = order.selected_options;
  generateControllerCombination(0, controller_count, available_controllers, work_area, selected_options,
                                actuated_joints);

  if (verbose_)
  {
    std::stringstream saj;
    std::stringstream sac;
    for (const std::string& available_controller : available_controllers)
      sac << available_controller << " ";
    for (const std::string& actuated_joint : actuated_joints)
      saj << actuated_joint << " ";
    RCLCPP_INFO(LOGGER, "Looking for %zu controllers among [ %s] that cover joints [ %s]. Found %zd options.",
                controller_count, sac.str().c_str(), saj.str().c_str(), selected_options.size());
  }

  // if none was found, this is a problem
  if (selected_options.empty())
    return false;

  // if only one was found, return it
  if (selected_options.size() == 1)
  {
    selected_controllers.swap(selected_options[0]);
    return true;
  }

  // if more options were found, evaluate them all and return the best one

  // count how many default controllers are used in each reported option, and how many joints are actuated in total by
  // the selected controllers,
  // to use that in the ranking of the options
  order.nrdefault.resize(selected_options.size(), 0);
  order.nrjoints.resize(selected_options.size(), 0);
  order.nractive.resize(selected_options.size(), 0);
  for (std::size_t i = 0; i < selected_options.size(); ++i)
  {
    for (std::size_t k = 0; k < selected_options[i].size(); ++k)
    {
      updateControllerState(selected_options[i][k], DEFAULT_CONTROLLER_INFORMATION_VALIDITY_AGE);
      const ControllerInformation& ci = known_controllers_[selected_options[i][k]];

      if (ci.state_.default_)
        order.nrdefault[i]++;
      if (ci.state_.active_)
        order.nractive[i]++;
      order.nrjoints[i] += ci.joints_.size();
    }
  }

  // define a bijection to compute the raking of the found options
  std::vector<std::size_t> bijection(selected_options.size(), 0);
  for (std::size_t i = 0; i < selected_options.size(); ++i)
    bijection[i] = i;

  // sort the options
  std::sort(bijection.begin(), bijection.end(), order);

  // depending on whether we are allowed to load & unload controllers,
  // we have different preference on deciding between options
  if (!manage_controllers_)
  {
    // if we can't load different options at will, just choose one that is already loaded
    for (std::size_t i = 0; i < selected_options.size(); ++i)
      if (areControllersActive(selected_options[bijection[i]]))
      {
        selected_controllers.swap(selected_options[bijection[i]]);
        return true;
      }
  }

  // otherwise, just use the first valid option
  selected_controllers.swap(selected_options[bijection[0]]);
  return true;
}

bool TrajectoryExecutionManager::isControllerActive(const std::string& controller)
{
  return areControllersActive(std::vector<std::string>(1, controller));
}

bool TrajectoryExecutionManager::areControllersActive(const std::vector<std::string>& controllers)
{
  for (const std::string& controller : controllers)
  {
    updateControllerState(controller, DEFAULT_CONTROLLER_INFORMATION_VALIDITY_AGE);
    std::map<std::string, ControllerInformation>::iterator it = known_controllers_.find(controller);
    if (it == known_controllers_.end() || !it->second.state_.active_)
      return false;
  }
  return true;
}

bool TrajectoryExecutionManager::selectControllers(const std::set<std::string>& actuated_joints,
                                                   const std::vector<std::string>& available_controllers,
                                                   std::vector<std::string>& selected_controllers)
{
  for (std::size_t i = 1; i <= available_controllers.size(); ++i)
    if (findControllers(actuated_joints, i, available_controllers, selected_controllers))
    {
      // if we are not managing controllers, prefer to use active controllers even if there are more of them
      if (!manage_controllers_ && !areControllersActive(selected_controllers))
      {
        std::vector<std::string> other_option;
        for (std::size_t j = i + 1; j <= available_controllers.size(); ++j)
          if (findControllers(actuated_joints, j, available_controllers, other_option))
          {
            if (areControllersActive(other_option))
            {
              selected_controllers = other_option;
              break;
            }
          }
      }
      return true;
    }
  return false;
}

bool TrajectoryExecutionManager::distributeTrajectory(TrajectoryExecutionContext* context)
{

  robot_trajectory::RobotTrajectory& robotTraj = context->trajectory_;
  const std::vector<std::string>& controllers = context->controllers_;
  std::vector<TrajectoryPart*>& parts = context->trajectory_parts_;

  parts.clear();

  auto group_ = robotTraj.getGroup();
  const std::vector<const moveit::core::JointModel*>& jnts =
      group_ ? group_->getActiveJointModels() : robotTraj.getRobotModel()->getActiveJointModels();

  std::set<std::string> actuated_joints_single;
  std::set<std::string> actuated_joints_mdof;

  for (const moveit::core::JointModel* active_joint : jnts)
  {
    if (active_joint->getVariableCount() == 1)
    {
      if (active_joint->isPassive() || active_joint->getMimic() != nullptr || active_joint->getType() == moveit::core::JointModel::FIXED)
        continue;
      actuated_joints_single.insert(active_joint->getName());
    }
    else
    {
      actuated_joints_mdof.insert(active_joint->getName());
    }
    // Store the indexes of the joints for later use in trajectory checking
    context->actuated_joint_indexes.push_back(robot_model_->getVariableIndex(active_joint->getName()));
  }

  for (std::size_t i = 0; i < controllers.size(); ++i)
  {
    parts.push_back(new TrajectoryPart());
    std::map<std::string, ControllerInformation>::iterator it = known_controllers_.find(controllers[i]);
    if (it == known_controllers_.end())
    {
      RCLCPP_ERROR_STREAM(LOGGER, "Controller " << controllers[i] << " not found.");
      return false;
    }
    std::vector<std::string> intersect_mdof;
    std::set_intersection(it->second.joints_.begin(), it->second.joints_.end(), actuated_joints_mdof.begin(),
                          actuated_joints_mdof.end(), std::back_inserter(intersect_mdof));
    std::vector<std::string> intersect_single;
    std::set_intersection(it->second.joints_.begin(), it->second.joints_.end(), actuated_joints_single.begin(),
                          actuated_joints_single.end(), std::back_inserter(intersect_single));
    if (intersect_mdof.empty() && intersect_single.empty())
      RCLCPP_WARN_STREAM(LOGGER, "No joints to be distributed for controller " << controllers[i]);
    {
      if (!intersect_mdof.empty())
      {
        parts[i]->actuated_joints.insert(parts[i]->actuated_joints.end(),intersect_mdof.begin(), intersect_mdof.end());

        std::vector<const moveit::core::JointModel*> joint_models(parts[i]->actuated_joints.size());
        for(std::size_t j = 0; j < parts[i]->actuated_joints.size();j++)
           joint_models[j] = robotTraj.getRobotModel()->getJointModel(parts[i]->actuated_joints[j]);
        for(std::size_t j = 0; j < robotTraj.size();j++){
          for(std::size_t z = 0; z < parts[i]->actuated_joints.size();z++){
            const double newVal = *robotTraj.getWayPoint(j).getJointVelocities(parts[i]->actuated_joints[z])*execution_velocity_scaling_;
            robotTraj.getWayPointPtr(j)->setJointVelocities(joint_models[z],&newVal);
          }
        }
        
      }
      if (!intersect_single.empty())
      {
        parts[i]->actuated_joints.insert(parts[i]->actuated_joints.end(),intersect_single.begin(), intersect_single.end());

        std::vector<const moveit::core::JointModel*> joint_models(parts[i]->actuated_joints.size());
        for(std::size_t j = 0; j < parts[i]->actuated_joints.size();j++)
           joint_models[j] = robotTraj.getRobotModel()->getJointModel(parts[i]->actuated_joints[j]);
        for(std::size_t j = 0; j < robotTraj.size();j++){
          for(std::size_t z = 0; z < parts[i]->actuated_joints.size();z++){
            const double newVal = *robotTraj.getWayPoint(j).getJointVelocities(parts[i]->actuated_joints[z])*execution_velocity_scaling_;
            robotTraj.getWayPointPtr(j)->setJointVelocities(joint_models[z],&newVal);
          }
        }
      }
    }
  }
  return true;
}

bool TrajectoryExecutionManager::validate(const TrajectoryExecutionContext& context) const
{
  if (allowed_start_tolerance_ == 0)  // skip validation on this magic number
    return true;

  RCLCPP_INFO(LOGGER, "Validating trajectory with allowed_start_tolerance %g", allowed_start_tolerance_);

  moveit::core::RobotStatePtr current_state;
  if (!csm_->waitForCurrentState(node_->now()) || !(current_state = csm_->getCurrentState()))
  {
    RCLCPP_WARN(LOGGER, "Failed to validate trajectory: couldn't receive full current joint state within 1s");
    return false;
  }
  
  for (const TrajectoryPart* part : context.trajectory_parts_)
  {
    moveit_msgs::msg::RobotTrajectory trajectory;
    context.trajectory_.getRobotTrajectoryMsg(trajectory,part->actuated_joints);
    if (!trajectory.joint_trajectory.points.empty())
    {
      // Check single-dof trajectory
      const std::vector<double>& positions = trajectory.joint_trajectory.points.front().positions;
      const std::vector<std::string>& joint_names = trajectory.joint_trajectory.joint_names;
      if (positions.size() != joint_names.size())
      {
        RCLCPP_ERROR(LOGGER, "Wrong trajectory: #joints: %zu != #positions: %zu", joint_names.size(), positions.size());
        return false;
      }

      for (std::size_t i = 0, end = joint_names.size(); i < end; ++i)
      {
        const moveit::core::JointModel* jm = current_state->getJointModel(joint_names[i]);
        if (!jm)
        {
          RCLCPP_ERROR_STREAM(LOGGER, "Unknown joint in trajectory: " << joint_names[i]);
          return false;
        }

        double cur_position = current_state->getJointPositions(jm)[0];
        double traj_position = positions[i];
        // normalize positions and compare
        jm->enforcePositionBounds(&cur_position);
        jm->enforcePositionBounds(&traj_position);
        if (jm->distance(&cur_position, &traj_position) > allowed_start_tolerance_)
        {
          RCLCPP_ERROR(LOGGER,
                       "\nInvalid Trajectory: start point deviates from current robot state more than %g"
                       "\njoint '%s': expected: %g, current: %g",
                       allowed_start_tolerance_, joint_names[i].c_str(), traj_position, cur_position);
          return false;
        }
      }
    }
    if (!trajectory.multi_dof_joint_trajectory.points.empty())
    {
      // Check multi-dof trajectory
      const std::vector<geometry_msgs::msg::Transform>& transforms =
          trajectory.multi_dof_joint_trajectory.points.front().transforms;
      const std::vector<std::string>& joint_names = trajectory.multi_dof_joint_trajectory.joint_names;
      if (transforms.size() != joint_names.size())
      {
        RCLCPP_ERROR(LOGGER, "Wrong trajectory: #joints: %zu != #transforms: %zu", joint_names.size(),
                     transforms.size());
        return false;
      }

      for (std::size_t i = 0, end = joint_names.size(); i < end; ++i)
      {
        const moveit::core::JointModel* jm = current_state->getJointModel(joint_names[i]);
        if (!jm)
        {
          RCLCPP_ERROR_STREAM(LOGGER, "Unknown joint in trajectory: " << joint_names[i]);
          return false;
        }

        // compute difference (offset vector and rotation angle) between current transform
        // and start transform in trajectory
        Eigen::Isometry3d cur_transform, start_transform;
        // computeTransform() computes a valid isometry by contract
        jm->computeTransform(current_state->getJointPositions(jm), cur_transform);
        start_transform = tf2::transformToEigen(transforms[i]);
        ASSERT_ISOMETRY(start_transform)  // unsanitized input, could contain a non-isometry
        Eigen::Vector3d offset = cur_transform.translation() - start_transform.translation();
        Eigen::AngleAxisd rotation;
        rotation.fromRotationMatrix(cur_transform.linear().transpose() * start_transform.linear());
        if ((offset.array() > allowed_start_tolerance_).any() || rotation.angle() > allowed_start_tolerance_)
        {
          RCLCPP_ERROR_STREAM(LOGGER, "\nInvalid Trajectory: start point deviates from current robot state more than "
                                          << allowed_start_tolerance_ << "\nmulti-dof joint '" << joint_names[i]
                                          << "': pos delta: " << offset.transpose()
                                          << " rot delta: " << rotation.angle());
          return false;
        }
      }
    }
  }
  return true;
}

bool TrajectoryExecutionManager::configure(TrajectoryExecutionContext** context,
                                          const moveit_msgs::msg::RobotTrajectory& trajectory,
                                          const std::vector<std::string>& controllers)
{  
  auto robotTraj = robot_trajectory::RobotTrajectory(robot_model_,trajectory.group_name).setRobotTrajectoryMsg(*(csm_->getCurrentState()),trajectory);
  //Do we even need to unwind?
  robotTraj.unwind(*(csm_->getCurrentState()));

  *context = new TrajectoryExecutionContext(robotTraj);

  if (robotTraj.getWayPointCount() == 0)
  {
    // empty trajectories don't need to configure anything
    return true;
  }

  std::set<std::string> actuated_joints;

  //TODO: Use robot_trajectory here too
  auto is_actuated = [this](const std::string& joint_name) -> bool {
    const moveit::core::JointModel* jm = robot_model_->getJointModel(joint_name);
    return (jm && !jm->isPassive() && !jm->getMimic() && jm->getType() != moveit::core::JointModel::FIXED);
  };
  for (const std::string& joint_name : trajectory.multi_dof_joint_trajectory.joint_names)
    if (is_actuated(joint_name))
      actuated_joints.insert(joint_name);
  for (const std::string& joint_name : trajectory.joint_trajectory.joint_names)
    if (is_actuated(joint_name))
      actuated_joints.insert(joint_name);

  if (actuated_joints.empty())
  {
    RCLCPP_WARN(LOGGER, "The trajectory to execute specifies no joints");
    return false;
  }

  if (controllers.empty())
  {
    bool retry = true;
    bool reloaded = false;
    while (retry)
    {
      retry = false;
      std::vector<std::string> all_controller_names;
      for (std::map<std::string, ControllerInformation>::const_iterator it = known_controllers_.begin();
           it != known_controllers_.end(); ++it)
        all_controller_names.push_back(it->first);
      if (selectControllers(actuated_joints, all_controller_names, (*context)->controllers_))
      {
        if (distributeTrajectory(*context))
          return true;
      }
      else
      {
        // maybe we failed because we did not have a complete list of controllers
        if (!reloaded)
        {
          reloadControllerInformation();
          reloaded = true;
          retry = true;
        }
      }
    }
  }
  else
  {
    // check if the specified controllers are valid names;
    // if they appear not to be, try to reload the controller information, just in case they are new in the system
    bool reloaded = false;
    for (const std::string& controller : controllers)
      if (known_controllers_.find(controller) == known_controllers_.end())
      {
        reloadControllerInformation();
        reloaded = true;
        break;
      }
    if (reloaded)
      for (const std::string& controller : controllers)
        if (known_controllers_.find(controller) == known_controllers_.end())
        {
          RCLCPP_ERROR(LOGGER, "Controller '%s' is not known", controller.c_str());
          return false;
        }
    if (selectControllers(actuated_joints, controllers, (*context)->controllers_))
    {
      if (distributeTrajectory(*context))
        return true;
    }
  }
  std::stringstream ss;
  for (const std::string& actuated_joint : actuated_joints)
    ss << actuated_joint << " ";
  RCLCPP_ERROR(LOGGER, "Unable to identify any set of controllers that can actuate the specified joints: [ %s]",
               ss.str().c_str());

  std::stringstream ss2;
  std::map<std::string, ControllerInformation>::const_iterator mi;
  for (mi = known_controllers_.begin(); mi != known_controllers_.end(); ++mi)
  {
    ss2 << "controller '" << mi->second.name_ << "' controls joints:\n";

    std::set<std::string>::const_iterator ji;
    for (ji = mi->second.joints_.begin(); ji != mi->second.joints_.end(); ++ji)
    {
      ss2 << "  " << *ji << '\n';
    }
  }
  RCLCPP_ERROR(LOGGER, "Known controllers and their joints:\n%s", ss2.str().c_str());
  return false;
}

moveit_controller_manager::ExecutionStatus TrajectoryExecutionManager::executeAndWait(bool auto_clear)
{
  execute(ExecutionCompleteCallback(), auto_clear);
  return waitForBlockingExecution();
}

void TrajectoryExecutionManager::stopExecutionInternal()
{
  // execution_state_mutex_ needs to have been locked by the caller
  used_handles_mutex_.lock();
  for (const moveit_controller_manager::MoveItControllerHandlePtr& used_handle : used_handles)
    try
    {
      used_handle->cancelExecution();
    }
    catch (std::exception& ex)
    {
      RCLCPP_ERROR(LOGGER, "Caught %s when canceling execution.", ex.what());
    }
  used_handles_mutex_.unlock();
}
void TrajectoryExecutionManager::stopContinuousExecution(){
    stop_continuous_execution_ = true;
    continuous_execution_condition_.notify_all();
}

void TrajectoryExecutionManager::stopBlockingExecution(bool auto_clear)
{

  if (!execution_complete_)
  {
    blocking_execution_state_mutex_.lock();
    if (!execution_complete_)
    {
      // we call cancel for all active handles; we know these are not being modified as we loop through them because of
      // the lock
      // we mark execution_complete_ as true ahead of time. Using this flag, executePart() will know that an external
      // trigger to stop has been received
      execution_complete_ = true;
      stopExecutionInternal();

      // we set the status here; executePart() will not set status when execution_complete_ is true ahead of time
      trajectories_[current_context_]->last_execution_status_ = moveit_controller_manager::ExecutionStatus::PREEMPTED;
      blocking_execution_state_mutex_.unlock();
      RCLCPP_INFO(LOGGER, "Stopped trajectory execution.");

      // wait for the execution thread to finish
      boost::mutex::scoped_lock lock(blocking_execution_thread_mutex_);
      if (blocking_execution_thread_)
      {
        blocking_execution_thread_->join();
        blocking_execution_thread_.reset();
      }

      if (auto_clear)
        clear();
    }
    else{
      blocking_execution_state_mutex_.unlock();
    }
      
  }
  else if (blocking_execution_thread_)  // just in case we have some thread waiting to be joined from some point in the past, we
                               // join it now
  {
    boost::mutex::scoped_lock lock(blocking_execution_thread_mutex_);
    if (blocking_execution_thread_)
    {
      blocking_execution_thread_->join();
      blocking_execution_thread_.reset();
    }
  }
}

void TrajectoryExecutionManager::execute(const ExecutionCompleteCallback& callback, bool auto_clear)
{
  execute(callback, PathSegmentCompleteCallback(), auto_clear);
}

void TrajectoryExecutionManager::execute(const ExecutionCompleteCallback& callback,
                                         const PathSegmentCompleteCallback& part_callback, bool auto_clear)
{
  stopBlockingExecution(false);

  // check whether first trajectory starts at current robot state
  if (!trajectories_.empty() && !validate(*trajectories_.front()))
  {
    last_execution_status_blocking_ = moveit_controller_manager::ExecutionStatus::ABORTED;
    if (auto_clear)
      clear();
    if (callback)
      callback(last_execution_status_blocking_);
    return;
  }

  // start the execution thread
  execution_complete_ = false;
  blocking_execution_thread_ = std::make_unique<boost::thread>(&TrajectoryExecutionManager::executeThread, this, callback,
                                                      part_callback, auto_clear);
}

moveit_controller_manager::ExecutionStatus TrajectoryExecutionManager::waitForBlockingExecution()
{
  {
    boost::unique_lock<boost::mutex> ulock(blocking_execution_state_mutex_);
    while (!execution_complete_)
      execution_complete_condition_.wait(ulock);
  }

  // this will join the thread for executing sequences of trajectories
  stopBlockingExecution(false);

  return last_execution_status_blocking_;
}

moveit_controller_manager::ExecutionStatus TrajectoryExecutionManager::waitForContinuousExecution()
{
  {
    boost::unique_lock<boost::mutex> ulock(continuous_execution_thread_mutex_);
    while (!continuous_execution_queue_.empty())
      continuous_execution_condition_.wait(ulock);
  }
  // this will join the thread for executing sequences of trajectories
  stopContinuousExecution();

  return last_execution_status_continuous_;
}

void TrajectoryExecutionManager::clear()
{
  if (execution_complete_)
  {
    for (TrajectoryExecutionContext* trajectory : trajectories_)
      delete trajectory;
    trajectories_.clear();
    {
      boost::mutex::scoped_lock slock(continuous_execution_thread_mutex_);
      while (!continuous_execution_queue_.empty())
      {
        delete continuous_execution_queue_.front();
        continuous_execution_queue_.pop_front();
      }
    }
  }
  else
    RCLCPP_ERROR(LOGGER, "Cannot pushToBlockingQueue a new trajectory while another is being executed");
}

void TrajectoryExecutionManager::executeThread(const ExecutionCompleteCallback& callback,
                                               const PathSegmentCompleteCallback& part_callback, bool auto_clear)
{
  // if we already got a stop request before we even started anything, we abort
  if (execution_complete_)
  {
    if (callback)
      callback(moveit_controller_manager::ExecutionStatus::ABORTED);
    return;
  }

  RCLCPP_INFO(LOGGER, "Starting trajectory execution ...");
  
  // execute each trajectory, one after the other (executePart() is blocking) or until one fails.
  // on failure, the status is set by executePart(). Otherwise, it will remain as set above (success)
  std::size_t i = 0;
  for (; i < trajectories_.size(); ++i)
  {
    // assume everything will be OK
    trajectories_[i]->last_execution_status_ = moveit_controller_manager::ExecutionStatus::SUCCEEDED;
    current_context_ = i;
    //Execute TrajectoryPart
    executePart(trajectories_[i],execution_complete_);
    current_context_ = -1;
    last_execution_status_blocking_ = trajectories_[i]->last_execution_status_;

    if (last_execution_status_blocking_ == moveit_controller_manager::ExecutionStatus::SUCCEEDED && part_callback)
      part_callback(i);
    if (last_execution_status_blocking_ != moveit_controller_manager::ExecutionStatus::SUCCEEDED || execution_complete_)
    {
      ++i;
      break;
    }
  }

  // only report that execution finished successfully when the robot actually stopped moving
  if (last_execution_status_blocking_ == moveit_controller_manager::ExecutionStatus::SUCCEEDED)
    waitForRobotToStop(*trajectories_[i - 1]);

  RCLCPP_INFO(LOGGER, "Completed trajectory execution with status %s ...", last_execution_status_blocking_.asString().c_str());

  // notify whoever is waiting for the event of trajectory completion
  blocking_execution_state_mutex_.lock();
  execution_complete_ = true;
  blocking_execution_state_mutex_.unlock();
  execution_complete_condition_.notify_all();

  // clear the paths just executed, if needed
  if (auto_clear)
    clear();

  // call user-specified callback
  if (callback)
    callback(last_execution_status_blocking_);
}

void TrajectoryExecutionManager::executePart(TrajectoryExecutionContext* context,bool& execution_complete_)
{
  // first make sure desired controllers are active
  if (ensureActiveControllers(context->controllers_))
  {
    // stop if we are already asked to do so
    
    if (execution_complete_){
      context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::ABORTED;
      return;
    }

    std::vector<moveit_controller_manager::MoveItControllerHandlePtr> handles;
    if (!execution_complete_)
    {
      handles.resize(context->controllers_.size());
      getContextHandles(*context, handles);

      for (std::size_t i = 0; i < context->trajectory_parts_.size(); ++i)
      {
        bool ok = false;
        try
        {
          moveit_msgs::msg::RobotTrajectory trajectory;
          context->trajectory_.getRobotTrajectoryMsg(trajectory,context->trajectory_parts_[i]->actuated_joints);
          ok = handles[i]->sendTrajectory(trajectory);
          context->start_time = context->trajectory_.getStartTime() > node_->now() ? context->trajectory_.getStartTime() : node_->now();
        }
        catch (std::exception& ex)
        {
          RCLCPP_ERROR(LOGGER, "Caught %s when sending trajectory to controller", ex.what());
        }
        if (!ok)
        {
          for (std::size_t j = 0; j < i; ++j)
            try
            {
              handles[j]->cancelExecution();
            }
            catch (std::exception& ex)
            {
              RCLCPP_ERROR(LOGGER, "Caught %s when canceling execution", ex.what());
            }
          RCLCPP_ERROR(LOGGER, "Failed to send trajectory part %zu of %zu to controller %s", i + 1,
                        context->trajectory_parts_.size(), handles[i]->getName().c_str());
          if (i > 0)
            RCLCPP_ERROR(LOGGER, "Cancelling previously sent trajectory parts");
          handles.clear();
          context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::ABORTED;
          return;
        }
      }
    }
    // Calculate expected duration
    rclcpp::Time current_time = node_->now();
    auto expected_trajectory_duration = rclcpp::Duration::from_seconds(0);
    for(std::size_t i = 0;i<context->trajectory_parts_.size();i++){

    // prefer controller-specific values over global ones if defined
      // TODO: the controller-specific parameters are static, but override
      //       the global ones are configurable via dynamic reconfigure
      std::map<std::string, double>::const_iterator scaling_it =
          controller_allowed_execution_duration_scaling_.find(context->controllers_[i]);
      const double current_scaling = scaling_it != controller_allowed_execution_duration_scaling_.end() ?
                                         scaling_it->second :
                                         allowed_execution_duration_scaling_;

      std::map<std::string, double>::const_iterator margin_it =
          controller_allowed_goal_duration_margin_.find(context->controllers_[i]);
      const double current_margin = margin_it != controller_allowed_goal_duration_margin_.end() ?
                                        margin_it->second :
                                        allowed_goal_duration_margin_;
    
    //TODO                                    
    expected_trajectory_duration = std::max( rclcpp::Duration::from_seconds(context->trajectory_.getDuration()) * current_scaling +  rclcpp::Duration::from_seconds(current_margin), expected_trajectory_duration);
    }

    RCLCPP_DEBUG_STREAM(LOGGER, "Populating the lists with " << handles.size() << " handles.");
    used_handles_mutex_.lock();
    for (std::size_t i = 0; i < context->trajectory_parts_.size(); ++i)
    {
      used_handles.push_back(handles[i]);
    }
    used_handles_mutex_.unlock();

    active_contexts_mutex_.lock();
    active_contexts_.push_back(context);
    active_contexts_mutex_.unlock();

    for (moveit_controller_manager::MoveItControllerHandlePtr& handle : handles)
    {
      if (execution_duration_monitoring_)
      {
        if (!handle->waitForExecution(expected_trajectory_duration))
          if (!execution_complete_ && node_->now() - current_time > expected_trajectory_duration)
          {
            RCLCPP_ERROR(LOGGER,
                        "Controller is taking too long to execute trajectory (the expected upper "
                        "bound for the trajectory execution was %lf seconds). Stopping trajectory.",
                        expected_trajectory_duration.seconds());
            stopExecutionInternal();  // this is really tricky. we can't call stopBlockingExecution() here, so we call the
                                      // internal function only
            context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::TIMED_OUT;
            break;
          }
      }
      else
        handle->waitForExecution();

      // if something made the trajectory stop, we stop this thread too
      if (execution_complete_)
      {
        break;
      }
      else if (handle->getLastExecutionStatus() != moveit_controller_manager::ExecutionStatus::SUCCEEDED)
      {
        RCLCPP_WARN_STREAM(LOGGER, "Controller handle " << handle->getName() << " reports status "
                                                        << handle->getLastExecutionStatus().asString());
        context->last_execution_status_ = handle->getLastExecutionStatus();
      }
    }
    //Surely there's a better way
    used_handles_mutex_.lock();
    for(std::size_t i = 0; i < context->trajectory_parts_.size(); ++i)
    {
      used_handles.erase(std::remove(used_handles.begin(), used_handles.end(), handles[i]), used_handles.end());
    }
    used_handles_mutex_.unlock();

    active_contexts_mutex_.lock();
    active_contexts_.erase(std::remove(active_contexts_.begin(), active_contexts_.end(), context), active_contexts_.end());
    active_contexts_mutex_.unlock();
  }
  else
  {
    context->last_execution_status_ = moveit_controller_manager::ExecutionStatus::ABORTED;
  }
}

bool TrajectoryExecutionManager::waitForRobotToStop(const TrajectoryExecutionContext& context, double wait_time)
{
  // skip waiting for convergence?
  if (allowed_start_tolerance_ == 0 || !wait_for_trajectory_completion_)
  {
    RCLCPP_INFO(LOGGER, "Not waiting for trajectory completion");
    return true;
  }

  auto start = std::chrono::system_clock::now();
  double time_remaining = wait_time;

  moveit::core::RobotStatePtr prev_state, cur_state;
  prev_state = csm_->getCurrentState();
  prev_state->enforceBounds();

  // assume robot stopped when 3 consecutive checks yield the same robot state
  unsigned int no_motion_count = 0;  // count iterations with no motion
  while (time_remaining > 0. && no_motion_count < 3)
  {
    if (!csm_->waitForCurrentState(node_->now(), time_remaining) || !(cur_state = csm_->getCurrentState()))
    {
      RCLCPP_WARN(LOGGER, "Failed to receive current joint state");
      return false;
    }
    cur_state->enforceBounds();
    std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - start;
    time_remaining = wait_time - elapsed_seconds.count();  // remaining wait_time

    // check for motion in effected joints of execution context
    bool moved = false;
    
    for (const TrajectoryPart* part : context.trajectory_parts_)
    {
      const std::size_t n = part->actuated_joints.size();

      for (std::size_t i = 0; i < n && !moved; ++i)
      {
        const moveit::core::JointModel* jm = cur_state->getJointModel(part->actuated_joints[i]);
        if (!jm)
          continue;  // joint vanished from robot state (shouldn't happen), but we don't care

        if (fabs(cur_state->getJointPositions(jm)[0] - prev_state->getJointPositions(jm)[0]) > allowed_start_tolerance_)
        {
          moved = true;
          no_motion_count = 0;
          break;
        }
      }
    }
    

    if (!moved)
      ++no_motion_count;

    std::swap(prev_state, cur_state);
  }

  return time_remaining > 0;
}

std::pair<int, int> TrajectoryExecutionManager::getCurrentExpectedTrajectoryIndex() const
{
  if (current_context_ < 0)
    return std::make_pair(-1, -1);
  int pos = trajectories_[current_context_]->getEstimatedIndex(node_->now());
  return std::make_pair(static_cast<int>(current_context_), pos);
}

const std::vector<TrajectoryExecutionManager::TrajectoryExecutionContext*>&
TrajectoryExecutionManager::getTrajectories() const
{
  return trajectories_;
}

moveit_controller_manager::ExecutionStatus TrajectoryExecutionManager::getLastExecutionStatusBlocking() const
{
  return last_execution_status_blocking_;
}

moveit_controller_manager::ExecutionStatus TrajectoryExecutionManager::getLastExecutionStatusContinuous() const
{
  return last_execution_status_continuous_;
}

bool TrajectoryExecutionManager::ensureActiveControllersForGroup(const std::string& group)
{
  const moveit::core::JointModelGroup* joint_model_group = robot_model_->getJointModelGroup(group);
  if (joint_model_group)
    return ensureActiveControllersForJoints(joint_model_group->getJointModelNames());
  else
    return false;
}

bool TrajectoryExecutionManager::ensureActiveControllersForJoints(const std::vector<std::string>& joints)
{
  std::vector<std::string> all_controller_names;
  for (std::map<std::string, ControllerInformation>::const_iterator it = known_controllers_.begin();
       it != known_controllers_.end(); ++it)
    all_controller_names.push_back(it->first);
  std::vector<std::string> selected_controllers;
  std::set<std::string> jset;
  for (const std::string& joint : joints)
  {
    const moveit::core::JointModel* jm = robot_model_->getJointModel(joint);
    if (jm)
    {
      if (jm->isPassive() || jm->getMimic() != nullptr || jm->getType() == moveit::core::JointModel::FIXED)
        continue;
      jset.insert(joint);
    }
  }

  if (selectControllers(jset, all_controller_names, selected_controllers))
    return ensureActiveControllers(selected_controllers);
  else
    return false;
}

bool TrajectoryExecutionManager::ensureActiveController(const std::string& controller)
{
  return ensureActiveControllers(std::vector<std::string>(1, controller));
}

bool TrajectoryExecutionManager::ensureActiveControllers(const std::vector<std::string>& controllers)
{
  reloadControllerInformation();

  updateControllersState(DEFAULT_CONTROLLER_INFORMATION_VALIDITY_AGE);

  if (manage_controllers_)
  {
    std::vector<std::string> controllers_to_activate;
    std::vector<std::string> controllers_to_deactivate;
    std::set<std::string> joints_to_be_activated;
    std::set<std::string> joints_to_be_deactivated;
    for (const std::string& controller : controllers)
    {
      std::map<std::string, ControllerInformation>::const_iterator it = known_controllers_.find(controller);
      if (it == known_controllers_.end())
      {
        std::stringstream stream;
        for (const auto& controller : known_controllers_)
        {
          stream << " `" << controller.first << "`";
        }
        RCLCPP_WARN_STREAM(LOGGER, "Controller " << controller << " is not known. Known controllers: " << stream.str());
        return false;
      }
      if (!it->second.state_.active_)
      {
        RCLCPP_DEBUG_STREAM(LOGGER, "Need to activate " << controller);
        controllers_to_activate.push_back(controller);
        joints_to_be_activated.insert(it->second.joints_.begin(), it->second.joints_.end());
        for (const std::string& overlapping_controller : it->second.overlapping_controllers_)
        {
          const ControllerInformation& ci = known_controllers_[overlapping_controller];
          if (ci.state_.active_)
          {
            controllers_to_deactivate.push_back(overlapping_controller);
            joints_to_be_deactivated.insert(ci.joints_.begin(), ci.joints_.end());
          }
        }
      }
      else
        RCLCPP_DEBUG_STREAM(LOGGER, "Controller " << controller << " is already active");
    }
    std::set<std::string> diff;
    std::set_difference(joints_to_be_deactivated.begin(), joints_to_be_deactivated.end(),
                        joints_to_be_activated.begin(), joints_to_be_activated.end(), std::inserter(diff, diff.end()));
    if (!diff.empty())
    {
      // find the set of controllers that do not overlap with the ones we want to activate so far
      std::vector<std::string> possible_additional_controllers;
      for (std::map<std::string, ControllerInformation>::const_iterator it = known_controllers_.begin();
           it != known_controllers_.end(); ++it)
      {
        bool ok = true;
        for (const std::string& controller_to_activate : controllers_to_activate)
          if (it->second.overlapping_controllers_.find(controller_to_activate) !=
              it->second.overlapping_controllers_.end())
          {
            ok = false;
            break;
          }
        if (ok)
          possible_additional_controllers.push_back(it->first);
      }

      // out of the allowable controllers, try to find a subset of controllers that covers the joints to be actuated
      std::vector<std::string> additional_controllers;
      if (selectControllers(diff, possible_additional_controllers, additional_controllers))
        controllers_to_activate.insert(controllers_to_activate.end(), additional_controllers.begin(),
                                       additional_controllers.end());
      else
        return false;
    }
    if (!controllers_to_activate.empty() || !controllers_to_deactivate.empty())
    {
      if (controller_manager_)
      {
        // load controllers to be activated, if needed, and reset the state update cache
        for (const std::string& controller_to_activate : controllers_to_activate)
        {
          ControllerInformation& ci = known_controllers_[controller_to_activate];
          ci.last_update_ = rclcpp::Time(0, 0, RCL_ROS_TIME);
        }
        // reset the state update cache
        for (const std::string& controller_to_activate : controllers_to_deactivate)
          known_controllers_[controller_to_activate].last_update_ = rclcpp::Time(0, 0, RCL_ROS_TIME);
        return controller_manager_->switchControllers(controllers_to_activate, controllers_to_deactivate);
      }
      else
        return false;
    }
    else
      return true;
  }
  else
  {
    std::set<std::string> originally_active;
    for (std::map<std::string, ControllerInformation>::const_iterator it = known_controllers_.begin();
         it != known_controllers_.end(); ++it)
      if (it->second.state_.active_)
        originally_active.insert(it->first);
    return std::includes(originally_active.begin(), originally_active.end(), controllers.begin(), controllers.end());
  }
}

void TrajectoryExecutionManager::loadControllerParams()
{
  // TODO: Revise XmlRpc parameter lookup
  // XmlRpc::XmlRpcValue controller_list;
  // if (node_->get_parameter("controller_list", controller_list) &&
  //     controller_list.getType() == XmlRpc::XmlRpcValue::TypeArray)
  // {
  //   for(std::size_t i = 0; i < controller_list.size(); ++i)  // NOLINT(modernize-loop-convert)
  //   {
  //     XmlRpc::XmlRpcValue& controller = controller_list[i];
  //     if (controller.hasMember("name"))
  //     {
  //       if (controller.hasMember("allowed_execution_duration_scaling"))
  //         controller_allowed_execution_duration_scaling_[std::string(controller["name"])] =
  //             controller["allowed_execution_duration_scaling"];
  //       if (controller.hasMember("allowed_goal_duration_margin"))
  //         controller_allowed_goal_duration_margin_[std::string(controller["name"])] =
  //             controller["allowed_goal_duration_margin"];
  //     }
  //   }
  // }
}

bool TrajectoryExecutionManager::checkCollisionBetweenTrajectories(const robot_trajectory::RobotTrajectory& new_trajectory,
                                                                   const TrajectoryExecutionContext* context)
{
  robot_trajectory::RobotTrajectory active_trajectory = context->trajectory_;

  RCLCPP_DEBUG(LOGGER, "Start checkCollision between trajectories");
  planning_scene_monitor::LockedPlanningSceneRO ps(planning_scene_monitor_);
  const collision_detection::AllowedCollisionMatrix acm = ps->getAllowedCollisionMatrix();

  collision_detection::CollisionRequest req;
  collision_detection::CollisionResult res;

  int before;
  int after;
  double blend;
  
  auto timeBefore = node_->now();
  double duration = (node_->now() - context->start_time).seconds();

  moveit::core::RobotState stateInterpol(robot_model_);

  const robot_trajectory::RobotTrajectory* shortTrajectory = nullptr; 
  const robot_trajectory::RobotTrajectory* longTrajectory = nullptr; 
  
  std::size_t i = 0;

  if(context->getEstimatedIndex(node_->now()) - active_trajectory.getWayPointCount() > new_trajectory.getWayPointCount()){
      shortTrajectory = &new_trajectory;
      longTrajectory = &active_trajectory;
      i = context->getEstimatedIndex(node_->now());
  }else{
    shortTrajectory = &active_trajectory;
    longTrajectory = &new_trajectory;
  }


  for(; i < longTrajectory->getWayPointCount();i++){
    const double* positionsLong = longTrajectory->getWayPoint(i).getVariablePositions();

    shortTrajectory->findWayPointIndicesForDurationAfterStart(duration  + longTrajectory->getWayPointDurationFromStart(i),before,after,blend);
    shortTrajectory->getWayPoint(before).interpolate(shortTrajectory->getWayPoint(after),blend,stateInterpol);
    double* positionsShort = stateInterpol.getVariablePositions();

    //Add all State positions into a single state
    for(auto joint : context->actuated_joint_indexes) positionsShort[joint] = positionsLong[joint];
    //Force update the State as we have changed internal variables
    stateInterpol.update(true);
    //Check if the calculated state is valid
    res.clear();
    ps->checkCollision(req, res, stateInterpol, acm);
    if(res.collision){
        RCLCPP_DEBUG(LOGGER,"Collision Detected");
        return false;
    }
  }
  RCLCPP_DEBUG(LOGGER, "Finished Collision Checking");
  return true;
}


bool TrajectoryExecutionManager::checkContextForCollisions(TrajectoryExecutionContext& context)
{
  // 2. Check that new trajectory does not collide with other active trajectories

  /* Approach: checking point by point :
        a. Get planning scene
        b. Update the planning scene with the first point of both trajectories
        c. Use robot's group name to check collisions of relevant links only 
        d. Check if the two states collide, if so, return, no need to check anything else
       - May be optimized by active trajectory backward, it is more likely that any collision would happen at the end of the trajectory than at the beginning (given that the planning return a valid trajectory).
  */
  active_contexts_mutex_.lock();
  for (const auto& currently_running_context : active_contexts_)
  {
    if (!checkCollisionBetweenTrajectories(context.trajectory_, currently_running_context))
    {
      // Push to backlog
      RCLCPP_DEBUG(LOGGER, "Collision found between trajectory with duration: %lf and trajectory with duration: %lf" ,context.trajectory_.getDuration(),
                                currently_running_context->trajectory_.getDuration());
      active_contexts_mutex_.unlock();
      return false;
    }
  }
  active_contexts_mutex_.unlock();
  return true;
}

bool TrajectoryExecutionManager::checkCollisionsWithCurrentState(moveit_msgs::msg::RobotTrajectory& trajectory)
{
  moveit::core::RobotStatePtr current_state;
  if (!csm_->waitForCurrentState(node_->now()) || !(current_state = csm_->getCurrentState()))
  {
    RCLCPP_DEBUG(LOGGER, "Failed to validate trajectory: couldn't receive full current joint state within 1s");
    return false;
  }
  planning_scene_monitor::LockedPlanningSceneRO ps(planning_scene_monitor_);
  if (jointTrajPointToRobotState(trajectory.joint_trajectory, trajectory.joint_trajectory.points.size()-1, *current_state))
  {
    moveit_msgs::msg::RobotState robot_state_msg;
    robotStateToRobotStateMsg(*current_state, robot_state_msg);
    if(!ps->isPathValid(robot_state_msg, trajectory, trajectory.group_name))
       {
      RCLCPP_DEBUG(LOGGER, "New trajectory collides with the current robot state. Abort!");
      //last_execution_status_continuous_ = moveit_controller_manager::ExecutionStatus::ABORTED;
      return false;
    }
  }

  return true;
}

void TrajectoryExecutionManager::getContextHandles(TrajectoryExecutionContext& context, std::vector<moveit_controller_manager::MoveItControllerHandlePtr>& handles)
{
  for (std::size_t i = 0; i < context.controllers_.size(); ++i)
  {
    moveit_controller_manager::MoveItControllerHandlePtr h;
    try
    {
      h = controller_manager_->getControllerHandle(context.controllers_[i]);
    }
    catch (std::exception& ex)
    {
      RCLCPP_ERROR(LOGGER, "%s caught when retrieving controller handle", ex.what());
    }
    if (!h)
    {
      context.last_execution_status_ = moveit_controller_manager::ExecutionStatus::ABORTED;
      RCLCPP_ERROR(LOGGER, "No controller handle for controller '%s'. Aborting.",
                      context.controllers_[i].c_str());
      handles.clear();
      break;
    }
    handles[i] = h;
  }
}

bool TrajectoryExecutionManager::validateAndExecuteContext(TrajectoryExecutionContext& context)
{
  RCLCPP_DEBUG(LOGGER, "Start validateAndExecuteContext");

  // Get the controller handles needed to execute the new trajectory
  std::vector<moveit_controller_manager::MoveItControllerHandlePtr> handles(context.controllers_.size());
  getContextHandles(context, handles);
  if (handles.empty())
  {
    RCLCPP_ERROR_STREAM(LOGGER, "Trajectory context had no controller handles??");  
    return false;
  }

  // Break out if flags set
  if (stop_continuous_execution_ || !run_continuous_execution_thread_)
  {
    return false;
  }


  RCLCPP_DEBUG(LOGGER, "DEBUG: Printing necessary handles for new traj");
  for (std::size_t i = 0; i < context.trajectory_parts_.size(); ++i)
  {
    RCLCPP_DEBUG_STREAM(LOGGER, "handle: " << (i+1) << " of " << context.trajectory_parts_.size() << " : " << handles[i]->getName());
    RCLCPP_DEBUG(LOGGER, "Next-up trajectory has duration %lf", context.trajectory_.getDuration());
  }
  
    // Check whether this trajectory starts at current robot state
  if (!validate(context))
  {
    RCLCPP_ERROR(LOGGER, "Trajectory became invalid before execution, abort.");
    context.execution_complete_callback(moveit_controller_manager::ExecutionStatus::ABORTED);
    return true;
  }

  // Check that controllers are not busy, wait for execution to finish if they are.
  for (std::size_t i = 0; i < context.trajectory_parts_.size(); ++i)
  {
    // Check if required handle is already in use
    used_handles_mutex_.lock();

    for(auto handle : used_handles){
      if (handles[i]->getName() == handle->getName())  // If controller is busy, return false so trajectory is pushed to backlog
      {
        RCLCPP_DEBUG_STREAM(LOGGER, "Handle " << handles[i]->getName() << " already in use: " << handle->getLastExecutionStatus().asString());
        return false;
      }
    }
    used_handles_mutex_.unlock();
  }
  // Skip trajectory if it collides with current state
  moveit_msgs::msg::RobotTrajectory trajectory;
  context.trajectory_.getRobotTrajectoryMsg(trajectory);
  if (!checkCollisionsWithCurrentState(trajectory))
  {
    RCLCPP_DEBUG(LOGGER, "Trajectory collides with current state. Cannot execute yet.");
    return false;
  }

  // Skip trajectory if it collides with other active trajectories
  if (!checkContextForCollisions(context)){
    return false;
  }
  
  std::thread([this,&context]() 
  {
    continuous_execution_condition_.notify_all();

    bool execution_complete_ = false;
    executePart(&context,execution_complete_);

    // Wait for the robot to stop moving
    waitForRobotToStop(context);

    //set the global execution status
    last_execution_status_continuous_ = context.last_execution_status_;
    RCLCPP_INFO(LOGGER, "Executed simultanous trajectory with status %s ...", last_execution_status_continuous_.asString().c_str());
    
    context.execution_complete_callback(last_execution_status_continuous_);

    //Check backlog for trajectories that can be executed again
    checkBacklog();

  }).detach();

  return true;
}

bool TrajectoryExecutionManager::hasCommonHandles(TrajectoryExecutionContext& context1, TrajectoryExecutionContext& context2)
{
  std::vector<moveit_controller_manager::MoveItControllerHandlePtr> ctx1_handles(context1.controllers_.size());
  std::vector<moveit_controller_manager::MoveItControllerHandlePtr> ctx2_handles(context2.controllers_.size());
  getContextHandles(context1, ctx1_handles);
  getContextHandles(context2, ctx2_handles);
  // Compare the two vectors to see if they have common objects
  return std::find_first_of(ctx1_handles.begin(), ctx1_handles.end(),ctx2_handles.begin(), ctx2_handles.end()) != ctx1_handles.end();
}

//TODO: Is this needed for real robots? In sim, it doesn't do anything...
void TrajectoryExecutionManager::updateTimestamps(TrajectoryExecutionContext& context)
{
  moveit::core::RobotStatePtr real_state = csm_->getCurrentState();

  robot_trajectory::RobotTrajectory currently_running_trajectory = context.trajectory_;

  //Get the duration of the running trajectory
  double durationFromStart = (node_->now() - context.start_time).seconds();
  RCLCPP_DEBUG(LOGGER, "Duration from Start: %f",durationFromStart);

  //Get the states inbetween which we should currently be based 
  //on our estimated time
  int before, after;double blend;
  currently_running_trajectory.findWayPointIndicesForDurationAfterStart(durationFromStart,before,after,blend);

  //Get states
  auto state_before = currently_running_trajectory.getWayPoint(before);
  auto state_after = currently_running_trajectory.getWayPoint(after);

  //Get the interpolated state that is the closest to the current state
  // and compare the timestamp of that state to the real timestamp
  const double* positionsA = state_before.getVariablePositions();
  const double* positionsB = state_after.getVariablePositions();
  const double* positionsReal = real_state->getVariablePositions();
  
  double lengthSqrAB = 0;
  double t = 0;

  for(auto j : context.actuated_joint_indexes){
    double diffAB = positionsB[j] - positionsA[j];
    double diffAReal = positionsReal[j] - positionsA[j];
    lengthSqrAB += pow(diffAB,2);
    t += diffAB*diffAReal;
  }
  double realBlend = t/lengthSqrAB;

  //Calculate the difference between real and estimated timestamp
  double time_diff = currently_running_trajectory.getWayPointDurationFromPrevious(after) * (realBlend - blend);

  RCLCPP_DEBUG(LOGGER, "Time difference (estimated/real) (Abs): %f",time_diff);
  RCLCPP_DEBUG(LOGGER, "index: %i",before);
  RCLCPP_DEBUG(LOGGER, "Before adjusting: %f",currently_running_trajectory.getWayPointDurationFromPrevious(after));
  //Adjust this timestamp with the calculated corretcion
  currently_running_trajectory.setWayPointDurationFromPrevious(after,currently_running_trajectory.getWayPointDurationFromPrevious(after) + time_diff);
  RCLCPP_DEBUG(LOGGER, "After adjusting: %f",currently_running_trajectory.getWayPointDurationFromPrevious(after));

}

//Not in use anymore, i'll leave it here if someone wants to utilize it
bool TrajectoryExecutionManager::isRemainingPathValid(TrajectoryExecutionContext& context)
{
  planning_scene_monitor::LockedPlanningSceneRO ps(planning_scene_monitor_);
  const collision_detection::AllowedCollisionMatrix acm = ps->getAllowedCollisionMatrix();

  std::size_t wpc = context.trajectory_.getWayPointCount();
  collision_detection::CollisionRequest req;
  req.group_name = context.trajectory_.getGroupName();
  for (std::size_t i = std::max(context.getEstimatedIndex(node_->now()) - 1, 0); i < wpc; ++i)
  {
    collision_detection::CollisionResult res;
    ps->checkCollisionUnpadded(req, res, context.trajectory_.getWayPoint(i), acm);
    if (res.collision || !ps->isStateFeasible(context.trajectory_.getWayPoint(i), false))
    {
      RCLCPP_INFO(LOGGER, "Remaining Path invalid");
      return false;
    }
  }
  return true;
}

bool TrajectoryExecutionManager::checkAllRemainingPaths()
{
  RCLCPP_DEBUG(LOGGER, "Start checking all trajectories");
  planning_scene_monitor::LockedPlanningSceneRO ps(planning_scene_monitor_);
  const collision_detection::AllowedCollisionMatrix acm = ps->getAllowedCollisionMatrix();

  moveit::core::RobotState combinedState(robot_model_);
  double* positions =  combinedState.getVariablePositions();

  if(!active_contexts_.empty()){
    collision_detection::CollisionRequest req;
    collision_detection::CollisionResult res;

    // Look ahead 1s ? How much? 
    for(std::size_t j = 0; j < 10; j++){
      rclcpp::Time timeStamp =node_->now() + rclcpp::Duration::from_seconds( 0.1 * j );
      for(std::size_t i = 0; i < active_contexts_.size(); i++)
      {
        // Get indexes for before and after the timestep and interpolate those values
        int before, after;double blend;
        active_contexts_[i]->trajectory_.findWayPointIndicesForDurationAfterStart((timeStamp - active_contexts_[i]->start_time).seconds(),before,after,blend);
        const double* positionsBefore = active_contexts_[i]->trajectory_.getWayPoint(before).getVariablePositions();
        const double* positionsAfter = active_contexts_[i]->trajectory_.getWayPoint(after).getVariablePositions();

        for(uint8_t& joint_idx :  active_contexts_[i]->actuated_joint_indexes){
          positions[joint_idx] = positionsBefore[joint_idx] + blend*(positionsAfter[joint_idx] - positionsBefore[joint_idx]);
        }
      }
      combinedState.update(true);

      res.clear();
      ps->checkCollision(req, res, combinedState, acm);

      if (res.collision)
      {       
        // Create mask to calculate combinations
        // active_contexts_.size() + 1 to also get the isolated contexts 
        std::string bitmask(2, 1); 
        bitmask.resize(active_contexts_.size() + 1, 0);  

        // Check all possible permutations of the contexts to get the colliding contexts
        // This is done in two steps to reduce overhead
        do {
          //Set the positions to zero
          combinedState.setToDefaultValues();
          for(std::size_t y = 0;y < robot_model_->getVariableCount();y++){
              positions[y] = 0;
          }

          // Go over it one more time, bitmask[i] ensures the combination of every context
          for(std::size_t i = 0; i < active_contexts_.size(); i++)
          {
            if (bitmask[i]) {
              // Get indexes for before and after the timestep and interpolate those values
              int before, after;double blend;
              active_contexts_[i]->trajectory_.findWayPointIndicesForDurationAfterStart((timeStamp - active_contexts_[i]->start_time).seconds(),before,after,blend);
              const double* positionsBefore = active_contexts_[i]->trajectory_.getWayPoint(before).getVariablePositions();
              const double* positionsAfter = active_contexts_[i]->trajectory_.getWayPoint(after).getVariablePositions();

              for(uint8_t& joint_idx :  active_contexts_[i]->actuated_joint_indexes){
                positions[joint_idx] = positionsBefore[joint_idx] + blend*(positionsAfter[joint_idx] - positionsBefore[joint_idx]);
              }
            }
          }
          combinedState.update(true);

          res.clear();
          ps->checkCollision(req, res, combinedState, acm);
          if (res.collision)
          {          
            RCLCPP_INFO(LOGGER, "Remaining Path invalid, cancelling execution.");
            //Extract the contexts that are colliding
            std::vector<int> colliding_contexts_;
            for(std::size_t i = 0; i < active_contexts_.size(); i++)
              if (bitmask[i]) colliding_contexts_.push_back(i);
            
            for(std::size_t i = 0; i < colliding_contexts_.size();i++){
              auto context = active_contexts_[colliding_contexts_[i]];
              if(!context->blocking){
              // Get all the handles for the context
              std::vector<moveit_controller_manager::MoveItControllerHandlePtr> handles(context->controllers_.size());
              getContextHandles(*context,handles);

              // Objects automatically get deleted from active_contexts and used handles in executePart
              // Cancel all executions
              for(auto& handle : handles){
                handle->cancelExecution();
              }
              break;
              }
            }
            return false;
          }
        } while (std::prev_permutation(bitmask.begin(), bitmask.end()));
      }
    }
  }
  RCLCPP_DEBUG(LOGGER, "Finished checking all Paths");
  return true;
}

}  // namespace trajectory_execution_manager
