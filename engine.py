import polars as pl
from .strategy import Strategy
from tqdm import tqdm

class Engine:
    def __init__(self, df) -> None:
        self.cache = df
        self.strategy = Strategy()
        self.stk_hold = {}
        self.cash = 1e10
        self.log = []
        self.baseline = {}
        self.baseline_log = []

    def backtest(self):
        
        self.strategy.on_init(self.cache)
        days = self.cache.sort(by=pl.col('date')).group_by('date')
        day_count = 0
        trade_count=0
        for date_time, day_data in tqdm(days):
            if day_count==0:
                self.buy_baseline(self.strategy.stk_pool, day_data)
            buy_in, sold_out = self.strategy.on_day(day_data, self.stk_hold)
            if day_count%self.strategy.trade_period==0:
                for id in sold_out:
                    self.Sell(id, day_data)
                for id,ratio in buy_in:
                    self.Buy(id, ratio, day_data)
                    trade_count+=1
            self.log.append(self.get_total_value(day_data))
            #self.baseline_log.append(self.get_baseline_value(day_data))
            day_count += 1
        print(f'you have trade {trade_count/2} times in total')
        return self.log, self.baseline_log

    def Sell(self, id, day_data):
        if not id in self.stk_hold:
            return
        data = day_data.filter(pl.col('stk_id')==id)
        price = (data[0, 'open'] + data[0, 'close'])/2
        val = self.stk_hold[id][0]
        self.cash += (price*val*(1-(1e-3)-(3e-4)) - val*(6e-4))
        del self.stk_hold[id]
        return
    
    def Buy(self, id, ratio, day_data):
        if self.cash<0:
            return
        buyin_cash = min(self.cash, 1e10*ratio)
        data = day_data.filter(pl.col('stk_id')==id)
        price = (data[0, 'open'] + data[0, 'close'])/2
        val = buyin_cash/(price+6e-4+price*3e-4)
        self.cash -= buyin_cash
        if id in self.stk_hold:
            self.stk_hold[id][0] += val
        else:
            self.stk_hold[id] = [val, price]
        return
    
    def get_total_value(self, day_data):
        ret = self.cash
        for id,(v,p) in self.stk_hold.items():
            data = day_data.filter(pl.col('stk_id')==id)
            price = (data[0, 'open'] + data[0, 'close'])/2
            ret += (price*v*(1-(1e-3)-(3e-4)) - v*(6e-4))
        return ret
    
    def set_pool(self):
        self.strategy.set_pool(self.cache['stk_id'].unique().to_list())
        return 
    
    def buy_baseline(self, stk_list, day_data):
        num = len(stk_list)
        buyin_cash = 1e10/num
        for id in stk_list:
            data = day_data.filter(pl.col('stk_id')==id)
            price = (data[0, 'open'] + data[0, 'close'])/2
            val = buyin_cash/(price+6e-4+price*3e-4)
            self.baseline[id] = val

    def get_baseline_value(self, day_data):
        ret = 0
        for id,v in self.baseline.items():
            data = day_data.filter(pl.col('stk_id')==id)
            price = (data[0, 'open'] + data[0, 'close'])/2
            ret += (price*v*(1-(1e-3)-(3e-4)) - v*(6e-4))
        return ret




