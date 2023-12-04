import polars as pl
from mysystem.strategy import Strategy
class MyStrategy(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.klines = pl.DataFrame()
        self.kline_num = 50
        self.signals = {}
        self.stk_num = 3577
        self.stk_pool = []
        self.trade_period = 1

    def on_init(self, stk_ids):
        self.stk_pool = stk_ids[2800:3000]
        self.stk_num = len(self.stk_pool)
        self.kline_num = 30*self.stk_num
        self.trade_period = 2

    def on_day(self, day_data, stk_hold):
        in_data = day_data.filter(pl.col('stk_id').is_in(self.stk_pool))
        if self.klines.shape[0]<self.kline_num:
            self.klines = self.klines.vstack(in_data)
            return [], []
        self.calculate_signal()
        self.klines = self.klines.vstack(in_data)
        self.klines = self.klines[self.stk_num:, :]
        return self.on_buy_in(), self.on_sold_out(stk_hold)
    
    def calculate_signal(self):
        gb = self.klines.group_by('stk_id', maintain_order=True)
        self.signals.clear()
        for stk_id,df in gb:
            df = df.with_columns(
                (pl.col('close')*pl.col('cumadj')/pl.col('cumadj').max()).alias('p')
            )
            
            df = df.with_columns(
                pl.col('p').rolling_mean(21).alias('ma5')
            )
            
            self.signals[stk_id] = df['ma5'].tail(1).to_list()[0] - df['p'].tail(1).to_list()[0]
        return
       
        
    def on_buy_in(self):
        t = sorted(self.signals.items(), key=lambda x:x[1], reverse=True)
        #print(t)
        return [(id, 0.04) for id,s in t[:5]]
    
    def on_sold_out(self, stk_hold):
        return [id for id,s in self.signals.items() if s<=0.005]
    
    def __name__(self):
        return '5DR'    
