# cn-permanent-portfolio

一个面向中国市场的永久投资组合回测工具，使用 `AkShare + SQLite + pandas + Streamlit` 实现数据获取、本地存储、阈值再平衡回测和本地可视化界面。

## 功能

- 支持基金、ETF、货币基金历史数据拉取
- 本地 SQLite 存储，按 `date + code` 去重
- 永久投资组合日频回测
- 偏离阈值触发再平衡
- Streamlit 页面支持修改资产、权重并运行回测
- 支持配置存档，保存起始投资时间和多组基金组合，便于快速切换和比较

## 默认资产

- `110020` 易方达沪深300ETF联接A
- `166016` 中欧纯债债券(LOF)C
- `000307` 易方达黄金ETF联接A
- `710502` 富安达现金通货币B

## 运行方式

```bash
pip install -r requirements.txt
streamlit run app.py
```

如果本机提示 `streamlit: command not found`，可以改用：

```bash
python3 -m streamlit run app.py
```

## 说明

- 默认初始资金为 `1.0`
- 默认目标权重为 `25% / 25% / 25% / 25%`
- 当任意资产权重高于 `35%` 或低于 `15%` 时，触发再平衡
- 货币基金优先尝试按历史收益重建净值；如果数据不可得，则退化为常数净值 `1.0` 的现金代理

