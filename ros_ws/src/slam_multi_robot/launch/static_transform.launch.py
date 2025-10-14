from launch import LaunchDescription
from launch_ros.actions import Node
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from ament_index_python.packages import get_package_share_directory
from launch.actions import ExecuteProcess
import subprocess

import yaml
import os

def generate_launch_description():
    
    # Declare the path to the robot configuration file
    config_path_arg = DeclareLaunchArgument(
        'config_file',
            default_value=os.path.join(
                get_package_share_directory('slam_multi_robot'),
                'config',
                'robots.yaml'
            ),
        description='Path to the robots configuration YAML file'
    )
    
    launch_description = LaunchDescription()
    launch_description.add_action(config_path_arg)
    
    config_file_path = LaunchConfiguration('config_file')
    
    

    
    # Leggi il file YAML per determinare quali robot abilitare
    pkg_slam_multi_robot = get_package_share_directory('slam_multi_robot')
    actual_config_path = os.path.join(pkg_slam_multi_robot, 'config', 'robots.yaml')
    
    try:
        with open(actual_config_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        # Itera attraverso tutti i robot
        for robot in config_data['robots']:
            # Controlla se il robot è abilitato
            if robot.get('enabled', False):
                # Crea lo static transform publisher per il robot
                static_transform_node = Node(
                    package='tf2_ros',
                    executable='static_transform_publisher',
                    name=f'static_transform_publisher_{robot["name"]}',
                    arguments=[
                        str(robot['x_pose']),
                        str(robot['y_pose']), 
                        str(robot['z_pose']),
                        '0', '0', '0',  # yaw, pitch, roll
                        'map',
                        f'{robot["name"]}/map'
                    ],
                    output='screen'
                )
                
                launch_description.add_action(static_transform_node)
                
    except Exception as e:
        print(f"Error reading config file: {e}")
        # Potresti voler gestire l'errore in modo diverso
    
    return launch_description