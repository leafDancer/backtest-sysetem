import polars as pl

class Strategy:
    def __init__(self) -> None:
        self.klines = pl.DataFrame()
        self.kline_num = 50
        self.signals = {}
        self.stk_num = 3577
        self.stk_pool = []
        self.trade_period = 1

    def on_init(self, df):
        self.stk_pool = df['stk_id'].unique(maintain_order=True).to_list()
        self.stk_num = len(self.stk_pool)
        self.kline_num = 65*self.stk_num
        self.trade_period = 7

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
        gb = gb.agg(
            [
                (((pl.col('close'))*pl.col('cumadj')/pl.col('cumadj').max()).rolling_mean(3).tail(1)-((pl.col('close'))*pl.col('cumadj')/pl.col('cumadj').max()).rolling_mean(60).tail(1)),
                pl.col('close').tail(1).alias('price')
             ]
        )

        for stk in gb.rows():
           
            self.signals[stk[0]] = (stk[1][0], stk[2][0])
        return
       
        
    def on_buy_in(self):
        t = sorted(self.signals.items(), key=lambda x:x[1][0])
        return [(id, 0.1) for id,(s,p) in t[:5]]
    
    def on_sold_out(self, stk_hold):
        return [id for id,(s1,p) in self.signals.items() if s1>=-0.01]
    
