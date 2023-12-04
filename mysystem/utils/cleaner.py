import polars as pl

class Cleaner():
    def __init__(self) -> None:
        pass

    def clean(self, df):
        id_list = df['stk_id'].unique(maintain_order=True).to_list()
        day_count = df.group_by('stk_id').count()
        max_day = day_count['count'].max()
        id_delete = day_count.filter(pl.col('count')<max_day)['stk_id'].to_list()
        id_list = list(set(id_list) - set(id_delete))
        df = df.filter(pl.col('stk_id').is_in(id_list))
        return df


