# 量化回测系统 23FALL PKU COURSE PROJECT

## 下载
`git clone https://github.com/leafDancer/backtest-sysetem.git`

## 配置环境
`conda env create -f requirements.yaml`

可能会涉及到库不全

-polars

`conda install -c conda-forge polars`

## 运行程序
test.ipynb文件包含使用系统的完整回测过程，具体操作细节可以参考notebook中的demo

## 补充说明
**strategy.py**  是一个基础5dr策略，继承策略模板，支持用户自定义

**./data**  文件夹内为课程提供的数据集

**./mysystem**  为回测系统，包含引擎、策略模板和工具包

*建议使用conda进行环境配置*

