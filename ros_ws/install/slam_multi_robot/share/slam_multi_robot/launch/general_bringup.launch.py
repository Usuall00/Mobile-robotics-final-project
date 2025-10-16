from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription, TimerAction
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.substitutions import FindPackageShare
from ament_index_python.packages import get_package_share_directory
import os


def generate_launch_description():

    # Percorsi completi ai singoli launch file
    pkg_slam_multi_robot = get_package_share_directory('slam_multi_robot')
    pkg_merging_pkg = get_package_share_directory('merging_pkg')
    robot_config_path = os.path.join(pkg_slam_multi_robot, 'config', 'robots.yaml')
    gazebo_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'gazebo_world.launch.py')
    slam_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'slam_bringup.launch.py')
    merge_maps_launch = os.path.join(pkg_merging_pkg, 'launch', 'map_merging.launch.py')
    navigation_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'navigation.launch.py')
    transform_launch = os.path.join(pkg_slam_multi_robot, 'launch', 'static_transform.launch.py')
    filter_launch = os.path.join(pkg_merging_pkg, 'launch', 'laser_filters.launch.py')

    # Launches Gazebo immediately
    gazebo = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(gazebo_launch)  
    )

    transform_launch = TimerAction(
        period=5.0,
        actions=[
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(transform_launch)
            )
        ]
    )

    filter_launch = TimerAction(
        period=10.0,
        actions=[
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(filter_launch)
            )
        ]
    )


    # Launches tb1_slam after 15 seconds
    tb1_slam = TimerAction(
        period=15.0,
        actions=[
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(slam_launch)
            )
        ]
    )

    # Launches merge_maps after 15 seconds
    merge_maps = TimerAction(
        period=15.0,
        actions=[
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(merge_maps_launch)
            )
        ]
    )
    
    # Launches navigation after 20 seconds
    navigation_launch = TimerAction(
        period=30.0,
        actions=[
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(navigation_launch)
            )
        ]
    )

    return LaunchDescription([
        gazebo,
        transform_launch,
        filter_launch,
        tb1_slam,
        merge_maps,
        navigation_launch
    ])
