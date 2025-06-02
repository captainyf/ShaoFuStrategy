from typing import List
import pandas as pd
import os
import time
from datetime import datetime

import akshare as ak
import itertools

class GetAKShareData(object):
    """获取股票数据"""
    def __init__(self) -> None:
        self.ch_stock_spot_df = None
        self.get_ch_stock_spot()

        if self.ch_stock_spot_df is None:
            print("初始化失败，请检查网络连接是否稳定或库版本")
            exit()

    # 获取A股所有股票实时行情数据
    def get_ch_stock_spot(self) -> None:
        try:
            print("开始获取A股代码和名称对应表")
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
            self.ch_stock_spot_df = stock_zh_a_spot_em_df

        except Exception as e:
            print(f"获取失败，请检查网络连接是否稳定或库版本: {str(e)}")

    # 获取A股某一指数的实时行情数据
    def get_ch_index_info(self):
        try:
            print("开始获取A股股票指数列表")
            index_info_df = ak.index_stock_info()

            print(f"A股的指数列表（共{len(index_info_df)}支）：")
            return index_info_df

        except Exception as e:
            print(f"获取失败，请检查网络连接是否稳定或库版本: {str(e)}")
            return None

    # 获取A股指数的成分股列表
    def get_ch_index_stock_cons(self, index_codes: str):
        try:
            print(f"开始获取指数{index_codes}的成分股")
            index_stock_cons_df = ak.index_stock_cons_sina(symbol=index_codes)
            index_stock_cons_df = index_stock_cons_df.sort_values(by=["code"]).reset_index(drop=True)

            print(f"指数{index_codes}的成分股列表（共{len(index_stock_cons_df)}支）：")
            return index_stock_cons_df

        except Exception as e:
            print(f"获取失败，请检查输入index_codes是否正确、网络连接是否稳定或库版本: {str(e)}")
            return None

    # 获取A股个股的历史日线数据
    def get_ch_stock_daily(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq"):
        try:
            # 获取股票历史日线数据
            print(f"开始获取股票{symbol}的历史日线数据")
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                                start_date=start_date, end_date=end_date, adjust=adjust)
            stock_zh_a_hist_df.columns = ['date', 'tic', 'open', 'close', 'high', 'low', 'volume', 'amount',
                                          'price_swing', 'change_pct', 'change', 'turnover_rate']

            # 插入day_of_week列（星期一为1）
            stock_zh_a_hist_df['date'] = pd.to_datetime(stock_zh_a_hist_df['date'])
            stock_zh_a_hist_df.insert(loc=2, column='day_of_week', value=stock_zh_a_hist_df['date'].dt.dayofweek + 1)
            stock_zh_a_hist_df["date"] = stock_zh_a_hist_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))

            # 插入tic_name列
            tic_name_df = self.ch_stock_spot_df[self.ch_stock_spot_df['代码'] == symbol]['名称'].values[0]
            stock_zh_a_hist_df.insert(loc=2, column='tic_name', value=tic_name_df)

            return stock_zh_a_hist_df

        except Exception as e:
            print(f"获取失败，请检查输入参数是否正确、网络连接是否稳定或库版本: {str(e)}")
            return None

    # 获取A股所有股票的历史日线数据
    def get_ch_stock_daily_all(self, start_date: str, end_date: str, adjust: str = "qfq"):
        stock_data_df = pd.DataFrame()
        stock_num = 0
        print("----- 开始下载个股数据 -----")
        ticker_list = self.ch_stock_spot_df['代码'].tolist()
        for ticker in ticker_list:
            stock_num += 1
            if stock_num % 10 == 0:
                print("   下载进度 : {}%".format(stock_num / len(ticker_list) * 100))

            data_tmp = self.get_ch_stock_daily(symbol=ticker, start_date=start_date, end_date=end_date, adjust=adjust)
            if data_tmp is not None:
                stock_data_df = pd.concat([stock_data_df, data_tmp], ignore_index=True)

        print("----- 下载完成 -----")

        # 删除为空的数据行
        stock_data_df = stock_data_df.dropna()
        stock_data_df = stock_data_df.reset_index(drop=True)
        stock_data_df = stock_data_df.sort_values(by=['date', 'tic']).reset_index(drop=True)

        return stock_data_df

    # 获取A股某一指数的股票历史日线数据
    def get_ch_index_stock_daily(self, index_codes: str, start_date: str, end_date: str, adjust: str = "qfq"):
        index_ticker_list = self.get_ch_index_stock_cons(index_codes=index_codes)
        if index_ticker_list is None:
            return None
        index_ticker_list = index_ticker_list.sort_values(by=["code"]).reset_index(drop=True)

        stock_data_df = pd.DataFrame()
        stock_num = 0
        for ticker in index_ticker_list["code"]:
            stock_num += 1
            if stock_num % 10 == 0:
                print("   下载进度 : {}%".format(stock_num / len(index_ticker_list) * 100))

            data_tmp = self.get_ch_stock_daily(symbol=ticker, start_date=start_date, end_date=end_date, adjust=adjust)
            if data_tmp is not None:
                stock_data_df = pd.concat([stock_data_df, data_tmp], ignore_index=True)

        print("----- 下载完成 -----")

        # 删除为空的数据行
        stock_data_df = stock_data_df.dropna()
        stock_data_df = stock_data_df.reset_index(drop=True)
        stock_data_df = stock_data_df.sort_values(by=['date', 'tic']).reset_index(drop=True)

        # stock_data_df = self.full_table(stock_data_df)

        return stock_data_df

    # 对当前时间段未上市的公司的所有行置零
    def full_table(self, df: pd.DataFrame) -> pd.DataFrame:
        # 获取 tic 和 date 的所有组合
        ticker_list = df['tic'].unique().tolist()
        date_list = list(pd.date_range(df['date'].min(), df['date'].max()).astype(str))
        combination = list(itertools.product(date_list, ticker_list))

        # 将 combination 和 df 按照 columns=["date", "tic"] 进行合并，将两表相交为空的行置零
        df_full = pd.DataFrame(combination, columns=["date", "tic"]).merge(df, on=["date", "tic"], how="left")
        df_full = df_full[df_full["date"].isin(df["date"])].fillna(0)
        df_full = df_full.sort_values(['date', 'tic'], ignore_index=True)

        return df_full

    # 获取按板块划分的指数历史信息

    # 获取按行业划分的指数历史信息

    # 获取按概念划分的指数历史信息


if __name__ == "__main__":
    akshare_data = GetAKShareData()
    df = akshare_data.get_ch_index_stock_daily(index_codes="000016", start_date="20220101", end_date="20220131")
    df.to_csv(os.path.join("../0_data_file", "data.csv"), index=False)
