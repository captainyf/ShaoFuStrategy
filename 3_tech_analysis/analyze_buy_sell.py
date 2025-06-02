import pandas as pd
import os

from utils.logger import logger


class AnalyzeBuySell(object):
    """根据BBI指标分析趋势
    Attributes:
        stock_data_path: 原始数据的文件地址
    """

    def __init__(self, stock_data_path: str) -> None:
        self.stock_df = pd.read_csv(stock_data_path, dtype={'tic': str})
        self.unique_ticker = self.stock_df["tic"].unique()
        self.threshold_up_down = 0.5  # 判断涨跌趋势的阈值
        self.threshold_j_buy = 20
        self.threshold_j_sell = 80

    def judge_trend(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        通过BBI指标的一阶导判断一段时间内股票的趋势
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 0 表示震荡趋势，1 表示上涨趋势，-1 表示下跌趋势
        """
        # 过滤指定时间段的数据
        period_date_df = self.stock_df[(self.stock_df['date'] >= start_date) & (self.stock_df['date'] <= end_date)]

        # 遍历每个股票
        trend_df = pd.DataFrame(columns=['tic', 'tic_name', 'trend'])
        for tic in self.unique_ticker:
            # 获取当前股票的BBI指标数据
            tic_data = period_date_df[period_date_df['tic'] == tic]
            if not tic_data.empty and 'bbi' in tic_data.columns:
                # 计算BBI指标的一阶导数
                bbi_derivative = tic_data['bbi'].diff().dropna()
                # 判断所选时间周期内的BBI指标的一阶导数的平均值
                avg_derivative = bbi_derivative.mean() if len(bbi_derivative) > 0 else 0

                # 根据一阶导数的平均值判断趋势
                if avg_derivative > self.threshold_up_down:
                    trend_df.loc[len(trend_df)] = [tic, tic_data['tic_name'].iloc[2], 1]
                elif avg_derivative < - self.threshold_up_down:
                    trend_df.loc[len(trend_df)] = [tic, tic_data['tic_name'].iloc[2], -1]
                else:
                    trend_df.loc[len(trend_df)] = [tic, tic_data['tic_name'].iloc[2], 0]
        return trend_df

    def judge_buy_or_sell(self) -> pd.DataFrame:
        """判断买卖点
        买入信号：j < 20（这个地方需要修改，具体修改成多少，Z瓷都知道哈）
        卖出信号：1） 止盈放飞：kdj_j > 80或站上BBI线2根后
                2） 止损全卖：收盘价close跌破止损价（该部分省略，端午快结束了，有点懒惰doge，有空再补充）
         :return: 买入信号为True，卖出信号为False
        """
        trading_signal = pd.DataFrame(columns=['tic', 'tic_name', 'trading_signal'])
        for tic in self.unique_ticker:
            # 获取当前股票的最新数据（假设数据按日期排序，最后一行为最新）
            tic_data = self.stock_df[self.stock_df['tic'] == tic].sort_values(by='date').tail(1)
            if not tic_data.empty and 'kdj_j' in tic_data.columns and tic_data['kdj_j'].iloc[0] < self.threshold_j_buy:
                trading_signal_state = True
            elif not tic_data.empty and 'kdj_j' in tic_data.columns and tic_data['kdj_j'].iloc[0] > self.threshold_j_sell:
                trading_signal_state = False
            else:
                trading_signal_state = None
            trading_signal.loc[len(trading_signal)] = [tic, tic_data['tic_name'].iloc[0], trading_signal_state]

        return trading_signal


if __name__ == "__main__":
    # 加载数据
    data_file_name = "data_sse50_tech.csv"
    data_path = os.path.join("../0_data_file", data_file_name)
    if not os.path.exists(data_path):
        logger.error("数据文件不存在，请检查")
        exit()

    # 初始化
    Analysor = AnalyzeBuySell(stock_data_path=data_path)

    # 判断涨跌趋势
    trend_df = Analysor.judge_trend(start_date="2025-05-01", end_date="2025-05-31")
    trend_df_save_file_name = data_file_name.split(".")[0] + "_trend.csv"
    trend_df.to_csv(os.path.join("../0_data_file", trend_df_save_file_name), index=False)

    # 判断买卖点
    trading_signals = Analysor.judge_buy_or_sell()
    trading_signals_save_file_name = data_file_name.split(".")[0] + "_trading.csv"
    trading_signals.to_csv(os.path.join("../0_data_file", trading_signals_save_file_name), index=False)