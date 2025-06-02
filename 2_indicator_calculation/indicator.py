import pandas as pd
import os
from stockstats import StockDataFrame as Sdf

from MyTT import BBI, KDJ, MACD, BOLL
from utils.logger import logger


class Indicator(object):
    """计算技术指标

    Attributes:
        data_path: 原始数据的文件地址
    """

    def __init__(self, stock_data_path: str) -> None:
        self.stock_df = pd.read_csv(stock_data_path, dtype={'tic': str})
        self.unique_ticker = self.stock_df["tic"].unique()

    def get_indicator_data(self):
        return self.stock_df

    def add_bbi(self) -> None:
        logger.info("Adding BBI")

        df = self.stock_df.copy()
        bbi_df = pd.DataFrame()
        for tic in self.unique_ticker:
            bbi_np = BBI(df[df.tic == tic]["close"].values)
            tmp_df = pd.DataFrame(bbi_np, columns=["bbi"])
            tmp_df["bbi"] = tmp_df["bbi"].apply(lambda x: round(x, 2))
            tmp_df["tic"] = tic
            tmp_df["date"] = self.stock_df[self.stock_df.tic == tic]["date"].to_list()
            bbi_df = pd.concat([bbi_df, tmp_df], ignore_index=True)
        df = df.merge(bbi_df, on=["tic", "date"], how="left")
        df = df.sort_values(by=["date", "tic"])
        self.stock_df = df

    def add_kdj(self) -> None:
        logger.info("Adding KDJ")

        df = self.stock_df.copy()
        kdj_df = pd.DataFrame()
        for tic in self.unique_ticker:
            k_np, d_np, j_np = KDJ(CLOSE=df[df.tic == tic]["close"].values,
                                   HIGH=df[df.tic == tic]["high"].values,
                                   LOW=df[df.tic == tic]["low"].values)
            tmp_df = pd.DataFrame({"kdj_k": k_np, "kdj_d": d_np, "kdj_j": j_np})
            tmp_df["kdj_k"] = tmp_df["kdj_k"].apply(lambda x: round(x, 2))
            tmp_df["kdj_d"] = tmp_df["kdj_d"].apply(lambda x: round(x, 2))
            tmp_df["kdj_j"] = tmp_df["kdj_j"].apply(lambda x: round(x, 2))
            tmp_df["tic"] = tic
            tmp_df["date"] = self.stock_df[self.stock_df.tic == tic]["date"].to_list()
            kdj_df = pd.concat([kdj_df, tmp_df], ignore_index=True)
        df = df.merge(kdj_df, on=["tic", "date"], how="left")
        df = df.sort_values(by=["date", "tic"])
        self.stock_df = df

    def add_macd(self) -> None:
        logger.info("Adding MACD")

        df = self.stock_df.copy()
        macd_df = pd.DataFrame()
        for tic in self.unique_ticker:
            dif_np, dea_np, macd_np = MACD(CLOSE=df[df.tic == tic]["close"].values)
            tmp_df = pd.DataFrame({"dif": dif_np, "dea": dea_np, "macd": macd_np})
            tmp_df["dif"] = tmp_df["dif"].apply(lambda x: round(x, 2))
            tmp_df["dea"] = tmp_df["dea"].apply(lambda x: round(x, 2))
            tmp_df["macd"] = tmp_df["macd"].apply(lambda x: round(x, 2))
            tmp_df["tic"] = tic
            tmp_df["date"] = self.stock_df[self.stock_df.tic == tic]["date"].to_list()
            macd_df = pd.concat([macd_df, tmp_df], ignore_index=True)
        df = df.merge(macd_df, on=["tic", "date"], how="left")
        df = df.sort_values(by=["date", "tic"])
        self.stock_df = df

    def add_boll(self) -> None:
        logger.info("Adding BOLL")

        df = self.stock_df.copy()
        boll_df = pd.DataFrame()
        for tic in self.unique_ticker:
            upper_np, mid_np, lower_np = BOLL(CLOSE=df[df.tic == tic]["close"].values)
            tmp_df = pd.DataFrame({"upper": upper_np, "mid": mid_np, "lower": lower_np})
            tmp_df["upper"] = tmp_df["upper"].apply(lambda x: round(x, 2))
            tmp_df["mid"] = tmp_df["mid"].apply(lambda x: round(x, 2))
            tmp_df["lower"] = tmp_df["lower"].apply(lambda x: round(x, 2))
            tmp_df["tic"] = tic
            tmp_df["date"] = self.stock_df[self.stock_df.tic == tic]["date"].to_list()
            boll_df = pd.concat([boll_df, tmp_df], ignore_index=True)
        df = df.merge(boll_df, on=["tic", "date"], how="left")
        df = df.sort_values(by=["date", "tic"])
        self.stock_df = df


if __name__ == "__main__":
    # 加载数据
    data_file_name = "data_sse50.csv"
    data_path = os.path.join("../0_data_file", data_file_name)
    if not os.path.exists(data_path):
        logger.error("数据文件不存在，请检查")
        exit()

    # 初始化
    indicator = Indicator(stock_data_path=data_path)

    # 计算指标
    indicator.add_bbi()
    indicator.add_kdj()
    indicator.add_macd()

    indicator_df = indicator.get_indicator_data().fillna(0)
    save_file_name = data_file_name.split(".")[0] + "_tech.csv"
    indicator_df.to_csv(os.path.join("../0_data_file", save_file_name), index=False)
