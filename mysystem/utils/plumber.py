from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from rich.console import Console
from rich.table import Table


class Plumber(object):
    def __init__(self) -> None:
        pass

    def parse_log(self, log):
        console = Console()
        table = Table(show_header=True, show_lines=True, title="回测结果")
        table.add_column("InitValue",width=12)
        table.add_column("FinalValue")
        table.add_column("MaxDrawdown")
        table.add_column("YearReturn")
        table.add_column("YearVolatility")
        table.add_column("YearSharpe")
        table.add_column("ExcessReturn")

        l1,l2 = log
        #净值曲线
        plt.plot(l1, label='strategy')
        plt.plot(l2, label='baseline')
        plt.title('total value versus baseline')
        plt.xlabel('day')
        plt.ylabel('value')
        #超额收益率
        excess_return = (l1[-1] - l2[-1])/l1[0]

        #最大回撤
        i = np.argmax((np.maximum.accumulate(l1)- l1)/np.maximum.accumulate(l1))
        j = np.argmax(l1[:i])
        max_drawdown = (l1[j] - l1[i]) / l1[j]
        plt.plot([i, j], [l1[i], l1[j]], 'o', color="r", markersize=5, label=f"max drawdown:{max_drawdown:.2%}")

        #年化收益率
        annual_return = (l1[-1]/l1[0])**(250/len(l1))-1

        #年化波动率
        r = pd.DataFrame({'r': l1})
        r = r.pct_change().dropna()
        r = r.r
        annual_volatility = np.sqrt(np.std(r)**2*252)

        #夏普比率
        sharpe = (annual_return - 0.03) / annual_volatility

        table.add_row(
            f"{l1[0]:.0f}",
            f"{l1[-1]:.0f}",
            f"{max_drawdown:.2%}",
            f"{annual_return:.2%}",
            f"{annual_volatility:.2%}",
            f"{sharpe:.2f}",
            f"{excess_return:.2%}"
        )
        console.print(table)
        plt.legend()
        plt.show()