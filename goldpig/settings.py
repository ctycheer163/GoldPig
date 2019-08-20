#coding:utf-8
import yaml
import os
import tushare as ts 

# 读取个人私有配置
SQL_SERVER = ''
filePath = os.path.dirname(__file__)
yamlPath = os.path.join(filePath, 'config.yaml')
if not os.path.exists(yamlPath):
    yamlPath =os.path.join(filePath, 'your_config.yaml')
with open(yamlPath,'r',encoding='utf-8') as f:
    yaml_cfg= yaml.load(f, Loader=yaml.SafeLoader)    
    SQL_SERVER = yaml_cfg['database']['SQL_SERVER']

# tushare
ts.set_token('795a959f32bfb58cda7f5b010824dcfa2f489252038557eb7453370d')


from loguru import logger
logger = logger.add("./log/goldpig.log")