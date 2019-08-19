#coding:utf-8
import yaml
import os

# 读取个人私有配置
filePath = os.path.dirname(__file__)
yamlPath = os.path.join(filePath, 'config.yaml')
if not os.path.exists(yamlPath):
    yamlPath =os.path.join(filePath, 'your_config.yaml')
with open(yamlPath,'r',encoding='utf-8') as f:
    yaml_cfg= yaml.load(f, Loader=yaml.SafeLoader)    
    SQL_SERVER = yaml_cfg['database']['SQL_SERVER']

