from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription, TimerAction
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.substitutions import FindPackageShare
from ament_index_python.packages import get_package_share_directory
import os
import yaml


def generate_launch_description():
    # Paths to individual launch files and configuration files
    pkg_slam_multi_robot = get_package_share_directory('slam_multi_robot')
    robot_config_path = os.path.join(pkg_slam_multi_robot, 'config', 'robots.yaml')
    tb1_slam_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'tb1_slam.launch.py')
    tb2_slam_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'tb2_slam.launch.py')
    tb3_slam_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'tb3_slam.launch.py')
    tb4_slam_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'tb4_slam.launch.py')

    slam_launch_files = {
        'tb1': tb1_slam_launch,
        'tb2': tb2_slam_launch,
        'tb3': tb3_slam_launch,
        'tb4': tb4_slam_launch,
    }

    ld = LaunchDescription()

    # Load robot configurations and launch per-robot nodes
    with open(robot_config_path, 'r') as f:
        robots = [r for r in yaml.safe_load(f)['robots'] if r.get('enabled', True)]

    for robot in robots:
        robot_name = robot['name']
        if robot_name in slam_launch_files:
            slam_action = IncludeLaunchDescription(
                PythonLaunchDescriptionSource(slam_launch_files[robot_name])
            )
            ld.add_action(slam_action)

    return ld
