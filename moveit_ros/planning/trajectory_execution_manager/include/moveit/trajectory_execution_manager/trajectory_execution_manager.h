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

#pragma once

#include <moveit/macros/class_forward.h>
#include <moveit/robot_model/robot_model.h>
#include <moveit/robot_model/link_model.h>
#include <moveit/robot_state/robot_state.h>
#include <moveit/robot_state/attached_body.h>
#include <moveit/planning_scene_monitor/planning_scene_monitor.h>
#include <moveit/planning_scene_monitor/current_state_monitor.h>
#include <moveit/planning_scene/planning_scene.h>
#include <moveit_msgs/msg/collision_object.hpp>
#include <moveit_msgs/msg/robot_trajectory.hpp>
#include <sensor_msgs/msg/joint_state.hpp>
#include <std_msgs/msg/string.hpp>
#include <rclcpp/rclcpp.hpp>
#include <moveit/controller_manager/controller_manager.h>
#include <boost/thread.hpp>
#include <pluginlib/class_loader.hpp>
#include <moveit/robot_trajectory/robot_trajectory.h>

#include <memory>
#include <deque>

#include "moveit_trajectory_execution_manager_export.h"

namespace trajectory_execution_manager
{
MOVEIT_CLASS_FORWARD(TrajectoryExecutionManager);  // Defines TrajectoryExecutionManagerPtr, ConstPtr, WeakPtr... etc

// Two modes:
// Managed controllers
// Unmanaged controllers: given the trajectory,
class MOVEIT_TRAJECTORY_EXECUTION_MANAGER_EXPORT TrajectoryExecutionManager
{
public:
  static const std::string EXECUTION_EVENT_TOPIC;

  /// Definition of the function signature that is called when the execution of all the pushed trajectories completes.
  /// The status of the overall execution is passed as argument
  typedef boost::function<void(const moveit_controller_manager::ExecutionStatus&)> ExecutionCompleteCallback;

  /// Definition of the function signature that is called when the execution of a pushed trajectory completes
  /// successfully.
  using PathSegmentCompleteCallback = boost::function<void(std::size_t)>;

  
  struct TrajectoryPart
  {
    std::vector<std::string> actuated_joints;
  };

  /// Data structure that represents information necessary to execute a trajectory
  struct TrajectoryExecutionContext
  {
    TrajectoryExecutionContext(robot_trajectory::RobotTrajectory robotTraj,bool blocking):trajectory_(robotTraj),blocking(blocking){
    }
    /// The controllers to use for executing the different trajectory parts;
    std::vector<std::string> controllers_;

    // The trajectory to execute, split in different parts (by joints), each set of joints corresponding to one
    // controller
    std::vector<TrajectoryPart*> trajectory_parts_;

    //The trajectory to execute
    robot_trajectory::RobotTrajectory trajectory_;

    // Last execution status
    moveit_controller_manager::ExecutionStatus last_execution_status_;

    //Timeout for backlog
    rclcpp::Duration backlog_timeout_ = rclcpp::Duration::from_seconds(0);

    // Callback to call when execution is finished
    ExecutionCompleteCallback execution_complete_callback;

    // Time at which the execution of the trajectory was started
    rclcpp::Time start_time;

    // If the trajectory is executed in the blocking queue or not
    bool blocking;

    // Get the estimated index of the trajectory 
    int getEstimatedIndex(const rclcpp::Time& time) const
    {
      int before, after;double blend;
      trajectory_.findWayPointIndicesForDurationAfterStart((time - start_time).seconds(),before,after,blend);
      return blend > 0.5 ? after : before;
    }
  };

  /// Load the controller manager plugin, start listening for events on a topic.
  TrajectoryExecutionManager(const rclcpp::Node::SharedPtr& node, const moveit::core::RobotModelConstPtr& robot_model,
                             const planning_scene_monitor::PlanningSceneMonitorPtr& planning_scene_monitor);

  /// Load the controller manager plugin, start listening for events on a topic.
   TrajectoryExecutionManager(const rclcpp::Node::SharedPtr& node,const moveit::core::RobotModelConstPtr& robot_model, 
                             const planning_scene_monitor::PlanningSceneMonitorPtr& planning_scene_monitor,
                             bool manage_controllers);

  /// Destructor. Cancels all running trajectories (if any)
  ~TrajectoryExecutionManager();

  /// If this function returns true, then this instance of the manager is allowed to load/unload/switch controllers
  bool isManagingControllers() const;

  /// Get the instance of the controller manager used (this is the plugin instance loaded)
  const moveit_controller_manager::MoveItControllerManagerPtr& getControllerManager() const;

  /** \brief Execute a named event (e.g., 'stop') */
  void processEvent(const std::string& event);

  /** \brief Make sure the active controllers are such that trajectories that actuate joints in the specified group can
     be executed.
      \note If manage_controllers_ is false and the controllers that happen to be active do not cover the joints in the
     group to be actuated, this function fails. */
  bool ensureActiveControllersForGroup(const std::string& group);

  /** \brief Make sure the active controllers are such that trajectories that actuate joints in the specified set can be
     executed.
      \note If manage_controllers_ is false and the controllers that happen to be active do not cover the joints to be
     actuated, this function fails. */
  bool ensureActiveControllersForJoints(const std::vector<std::string>& joints);

  /** \brief Make sure a particular controller is active.
      \note If manage_controllers_ is false and the controllers that happen to be active to not include the one
     specified as argument, this function fails. */
  bool ensureActiveController(const std::string& controller);

  /** \brief Make sure a particular set of controllers are active.
      \note If manage_controllers_ is false and the controllers that happen to be active to not include the ones
     specified as argument, this function fails. */
  bool ensureActiveControllers(const std::vector<std::string>& controllers);

  /** \brief Check if a controller is active */
  bool isControllerActive(const std::string& controller);

  /** \brief Check if a set of controllers are active */
  bool areControllersActive(const std::vector<std::string>& controllers);

  /// Add a trajectory for future execution. Optionally specify a controller to use for the trajectory. If no controller
  /// is specified, a default is used.
  bool pushToBlockingQueue(const moveit_msgs::msg::RobotTrajectory& trajectory, const std::string& controller = "");

  /// Add a trajectory for future execution. Optionally specify a controller to use for the trajectory. If no controller
  /// is specified, a default is used.
  bool pushToBlockingQueue(const trajectory_msgs::msg::JointTrajectory& trajectory, const std::string& controller = "");

  /// Add a trajectory for future execution. Optionally specify a set of controllers to consider using for the
  /// trajectory. Multiple controllers can be used simultaneously
  /// to execute the different parts of the trajectory. If multiple controllers can be used, preference is given to the
  /// already loaded ones.
  /// If no controller is specified, a default is used.
  bool pushToBlockingQueue(const trajectory_msgs::msg::JointTrajectory& trajectory, const std::vector<std::string>& controllers);

  /// Add a trajectory for future execution. Optionally specify a set of controllers to consider using for the
  /// trajectory. Multiple controllers can be used simultaneously
  /// to execute the different parts of the trajectory. If multiple controllers can be used, preference is given to the
  /// already loaded ones.
  /// If no controller is specified, a default is used.
  bool pushToBlockingQueue(const moveit_msgs::msg::RobotTrajectory& trajectory, const std::vector<std::string>& controllers);

  /// Get the trajectories to be executed
  const std::vector<TrajectoryExecutionContext*>& getTrajectories() const;

  /// Start the execution of pushed trajectories; this does not wait for completion, but calls a callback when done.
  void execute(const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(), bool auto_clear = true);

  /// Start the execution of pushed trajectories; this does not wait for completion, but calls a callback when done. A
  /// callback is also called for every trajectory part that completes successfully.
  void execute(const ExecutionCompleteCallback& callback, const PathSegmentCompleteCallback& part_callback,
               bool auto_clear = true);

  /// This is a blocking call for the execution of the passed in trajectories. This just calls execute() and
  /// waitForExecution()
  moveit_controller_manager::ExecutionStatus executeAndWait(bool auto_clear = true);

  /// Add a trajectory for immediate execution. Optionally specify a controller to use for the trajectory. If no
  /// controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const moveit_msgs::msg::RobotTrajectory& trajectory, const std::string& controller = "", const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& backlog_timeout_ = rclcpp::Duration::from_seconds(60));

  /// Add a trajectory for immediate execution. Optionally specify a controller to use for the trajectory. If no
  /// controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const trajectory_msgs::msg::JointTrajectory& trajectory, const std::string& controller = "", const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& backlog_timeout_ = rclcpp::Duration::from_seconds(60));

  /// Add a trajectory that consists of a single state for immediate execution. Optionally specify a controller to use
  /// for the trajectory.
  /// If no controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const sensor_msgs::msg::JointState& state, const std::string& controller = "", const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& backlog_timeout_ = rclcpp::Duration::from_seconds(60));

  /// Add a trajectory for immediate execution. Optionally specify a set of controllers to consider using for the
  /// trajectory. Multiple controllers can be used simultaneously
  /// to execute the different parts of the trajectory. If multiple controllers can be used, preference is given to the
  /// already loaded ones.
  /// If no controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const trajectory_msgs::msg::JointTrajectory& trajectory, const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& backlog_timeout_ = rclcpp::Duration::from_seconds(60));

  /// Add a trajectory for immediate execution. Optionally specify a set of controllers to consider using for the
  /// trajectory. Multiple controllers can be used simultaneously
  /// to execute the different parts of the trajectory. If multiple controllers can be used, preference is given to the
  /// already loaded ones.
  /// If no controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const moveit_msgs::msg::RobotTrajectory& trajectory, const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& timeout = rclcpp::Duration::from_seconds(60));

  /// Add a trajectory that consists of a single state for immediate execution. Optionally specify a set of controllers
  /// to consider using for the trajectory.
  /// Multiple controllers can be used simultaneously to execute the different parts of the trajectory. If multiple
  /// controllers can be used, preference
  /// is given to the already loaded ones. If no controller is specified, a default is used. This call is non-blocking.
  bool pushAndExecuteSimultaneous(const sensor_msgs::msg::JointState& state, const std::vector<std::string>& controllers, const ExecutionCompleteCallback& callback = ExecutionCompleteCallback(),const rclcpp::Duration& timeout = rclcpp::Duration::from_seconds(60));

  /// Wait until the execution is complete. This only works for executions started by execute().  If you call this after
  /// pushAndExecuteSimultaneous(), it will immediately stop execution.
  moveit_controller_manager::ExecutionStatus waitForBlockingExecution();

  /// Get the state that the robot is expected to be at, given current time, after execute() has been called. The return
  /// value is a pair of two index values:
  /// first = the index of the trajectory to be executed (in the order pushToBlockingQueue() was called), second = the index of the
  /// point within that trajectory.
  /// Values of -1 are returned when there is no trajectory being executed, or if the trajectory was passed using
  /// pushAndExecuteSimultaneous().
  std::pair<int, int> getCurrentExpectedTrajectoryIndex() const;

  /// Return the controller status for the last attempted execution
  moveit_controller_manager::ExecutionStatus getLastExecutionStatus() const;

  /// Stop all blocking executions
  void stopBlockingExecution(bool auto_clear = true);

  /// Stop all Continuous executions
  void stopContinuousExecution();


  /// Clear the trajectories to execute
  void clear();

  /// Enable or disable the monitoring of trajectory execution duration. If a controller takes
  /// longer than expected, the trajectory is canceled
  void enableExecutionDurationMonitoring(bool flag);

  /// When determining the expected duration of a trajectory, this multiplicative factor is applied
  /// to get the allowed duration of execution
  void setAllowedExecutionDurationScaling(double scaling);

  /// When determining the expected duration of a trajectory, this multiplicative factor is applied
  /// to allow more than the expected execution time before triggering trajectory cancel
  void setAllowedGoalDurationMargin(double margin);

  /// Before sending a trajectory to a controller, scale the velocities by the factor specified.
  /// By default, this is 1.0
  void setExecutionVelocityScaling(double scaling);

  /// Set joint-value tolerance for validating trajectory's start point against current robot state
  void setAllowedStartTolerance(double tolerance);

  /// Enable or disable waiting for trajectory completion
  void setWaitForTrajectoryCompletion(bool flag);

  rclcpp::Node::SharedPtr getControllerManagerNode()
  {
    return controller_mgr_node_;
  }

private:
  struct ControllerInformation
  {
    std::string name_;
    std::set<std::string> joints_;
    std::set<std::string> overlapping_controllers_;
    moveit_controller_manager::MoveItControllerManager::ControllerState state_;
    rclcpp::Time last_update_{ 0, 0, RCL_ROS_TIME };

    bool operator<(ControllerInformation& other) const
    {
      if (joints_.size() != other.joints_.size())
        return joints_.size() < other.joints_.size();
      return name_ < other.name_;
    }
  };

  void initialize();

  void reloadControllerInformation();

  /// Validate first point of trajectory matches current robot state
  bool validate(const TrajectoryExecutionContext& context) const;
  TrajectoryExecutionContext* configure(const moveit_msgs::msg::RobotTrajectory& trajectory,const std::vector<std::string>& controllers,bool blocking,const rclcpp::Duration& backlog_timeout_ = rclcpp::Duration::from_seconds(60));

  void updateControllersState(const rclcpp::Duration& age);
  void updateControllerState(const std::string& controller, const rclcpp::Duration& age);
  void updateControllerState(ControllerInformation& ci, const rclcpp::Duration& age);

  bool distributeTrajectory(robot_trajectory::RobotTrajectory& robotTraj,
                            const std::vector<std::string>& controllers,
                            std::vector<TrajectoryPart*>& parts);

  bool findControllers(const std::set<std::string>& actuated_joints, std::size_t controller_count,
                       const std::vector<std::string>& available_controllers,
                       std::vector<std::string>& selected_controllers);
  bool checkControllerCombination(std::vector<std::string>& controllers, const std::set<std::string>& actuated_joints);
  void generateControllerCombination(std::size_t start_index, std::size_t controller_count,
                                     const std::vector<std::string>& available_controllers,
                                     std::vector<std::string>& selected_controllers,
                                     std::vector<std::vector<std::string> >& selected_options,
                                     const std::set<std::string>& actuated_joints);
  bool selectControllers(const std::set<std::string>& actuated_joints,
                         const std::vector<std::string>& available_controllers,
                         std::vector<std::string>& selected_controllers);

  void executeThread(const ExecutionCompleteCallback& callback, const PathSegmentCompleteCallback& part_callback,
                     bool auto_clear);
  void executePart(TrajectoryExecutionContext* context,bool& execution_complete_);
  bool waitForRobotToStop(const TrajectoryExecutionContext& context, double wait_time = 1.0);
  void continuousExecutionThread();

  void stopExecutionInternal();

  void receiveEvent(const std_msgs::msg::String::SharedPtr event);

  void loadControllerParams();

  // Name of this class for logging
  const std::string name_ = "trajectory_execution_manager";

  // Removes controllers/handles and trajectory contexts that have finished or aborted execution
  void updateActiveHandlesAndContexts(std::set<moveit_controller_manager::MoveItControllerHandlePtr>& used_handles);

  void checkBacklog();
  void checkBacklogExpiration();

  bool checkCollisionBetweenTrajectories(const robot_trajectory::RobotTrajectory& new_trajectory, const TrajectoryExecutionContext* context);

  // Check for collisions/controller issues, then send the trajectory for execution
  bool validateAndExecuteContext(TrajectoryExecutionContext& context);

  bool checkContextForCollisions(TrajectoryExecutionContext& context);

  void getContextHandles(TrajectoryExecutionContext& context, std::vector<moveit_controller_manager::MoveItControllerHandlePtr>& handles);

  void updateTimestamps(TrajectoryExecutionContext& context);

  bool isRemainingPathValid(TrajectoryExecutionContext& context);

  /**
   * @brief Validate whether two trajectory context require a common controller handle
   * @param context1 The first trajectory context
   * @param context2 The second trajectory context
   * @return true if there is any common controller handle, false otherwise
  */
  bool hasCommonHandles(TrajectoryExecutionContext& context1, TrajectoryExecutionContext& context2);

  bool checkCollisionsWithCurrentState(moveit_msgs::msg::RobotTrajectory& trajectory);

  rclcpp::Node::SharedPtr node_;
  rclcpp::Node::SharedPtr controller_mgr_node_;
  std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> private_executor_;
  std::thread private_executor_thread_;
  moveit::core::RobotModelConstPtr robot_model_;
  planning_scene_monitor::PlanningSceneMonitorPtr planning_scene_monitor_;
  planning_scene_monitor::CurrentStateMonitorPtr csm_;
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr event_topic_subscriber_;
  std::map<std::string, ControllerInformation> known_controllers_;
  bool manage_controllers_;

  // Thread used to execute trajectories using the execute() command. This is blocking and executes only one TrajectoryContext at a time.
  std::unique_ptr<boost::thread> blocking_execution_thread_;

  // Thread used to execute trajectories using pushAndExecuteSimultaneous(). This executes multiple TrajectoryContexts at the same time.
  std::unique_ptr<boost::thread> continuous_execution_thread_;

  boost::mutex execution_state_mutex_;
  boost::mutex continuous_execution_thread_mutex_;
  boost::mutex blocking_execution_thread_mutex_;

  boost::condition_variable continuous_execution_condition_;

  // this condition is used to notify the completion of execution for given trajectories
  boost::condition_variable execution_complete_condition_;

  moveit_controller_manager::ExecutionStatus last_execution_status_blocking_;
  int current_context_;
  std::vector<rclcpp::Time> time_index_;  // used to find current expected trajectory location
  mutable boost::mutex time_index_mutex_;
  bool execution_complete_;

  bool stop_continuous_execution_;
  bool run_continuous_execution_thread_;
  std::vector<TrajectoryExecutionContext*> trajectories_;
  std::deque<TrajectoryExecutionContext*> continuous_execution_queue_;

  std::unique_ptr<pluginlib::ClassLoader<moveit_controller_manager::MoveItControllerManager> > controller_manager_loader_;
  moveit_controller_manager::MoveItControllerManagerPtr controller_manager_;

  std::set<TrajectoryExecutionContext*> active_contexts_;
  std::set<moveit_controller_manager::MoveItControllerHandlePtr> used_handles;
  std::deque<std::pair<TrajectoryExecutionContext*, rclcpp::Time>> backlog;


  bool verbose_;

  bool execution_duration_monitoring_;
  // 'global' values
  double allowed_execution_duration_scaling_;
  double allowed_goal_duration_margin_;
  // controller-specific values
  // override the 'global' values
  std::map<std::string, double> controller_allowed_execution_duration_scaling_;
  std::map<std::string, double> controller_allowed_goal_duration_margin_;

  double allowed_start_tolerance_;  // joint tolerance for validate(): radians for revolute joints
  double execution_velocity_scaling_;
  bool wait_for_trajectory_completion_;

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr callback_handler_;
};
}  // namespace trajectory_execution_manager
