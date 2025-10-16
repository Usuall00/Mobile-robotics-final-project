import os
from launch import LaunchDescription
from launch_ros.actions import Node
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
import yaml
from ament_index_python.packages import get_package_share_directory

def generate_launch_description():
    ld = LaunchDescription()
    
    use_sim_time = LaunchConfiguration("use_sim_time", default="true")
    
    declare_use_sim_time = DeclareLaunchArgument(
        "use_sim_time", 
        default_value="true",
        description="Use simulation clock"
    )
    
    # Carica le configurazioni dei robot dal file YAML
    config_path = os.path.join(
        get_package_share_directory("slam_multi_robot"),
        'config',
        'robots.yaml'
    )
    
    filter_nodes = []
    enabled_robots = []
    
    try:
        with open(config_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        if 'robots' in config_data:
            for robot in config_data['robots']:
                if robot['enabled']:
                    # Crea un filtro per ogni robot abilitato
                    node = Node(
                        package="merging_pkg",
                        executable="robot_laser_filter",
                        name=f"{robot['name']}_laser_filter",
                        parameters=[{
                            'use_sim_time': use_sim_time,
                            'robot_name': robot['name'],
                            'robot_radius': 0.3,
                            'safety_margin': 0.2,
                            'filter_enabled': True
                        }],
                        output="screen"
                    )
                    filter_nodes.append(node)
                    enabled_robots.append(robot['name'])
                    print(f"✅ Added laser filter for {robot['name']}")
                else:
                    print(f"⏭️  Skipping disabled robot: {robot['name']}")
        
        print(f"🎯 Total laser filters: {len(filter_nodes)} for robots: {enabled_robots}")
    
    except Exception as e:
        print(f"❌ Error loading robot configurations: {e}")
        print("🔄 Using default robot configurations...")
        
        # Fallback ai robot di default
        default_robots = ['tb1', 'tb2']
        
        for robot_name in default_robots:
            node = Node(
                package="merging_pkg",
                executable="robot_laser_filter",
                name=f"{robot_name}_laser_filter",
                parameters=[{
                    'use_sim_time': use_sim_time,
                    'robot_name': robot_name,
                    'robot_radius': 0.3,
                    'safety_margin': 0.2,
                    'filter_enabled': True
                }],
                output="screen"
            )
            filter_nodes.append(node)
            enabled_robots.append(robot_name)
    
    ld.add_action(declare_use_sim_time)
    
    for filter_node in filter_nodes:
        ld.add_action(filter_node)
    
    return ld