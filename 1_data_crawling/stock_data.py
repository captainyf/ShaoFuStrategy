import os
from datetime import datetime

# sys.path.append("..")

from get_akshare_data import GetAKShareData


class StockData(object):
    """用来获取数据的类

    Attributes:
        stock_list: 股票代码
    """

    def __init__(self) -> None:
        self.data_dir = "../0_data_file"
        self.create_data_dir()
        self.akshare_data = GetAKShareData()

    def create_data_dir(self) -> None:
        """创建存储数据的文件夹"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            print("{} 文件夹创建成功!".format(self.data_dir))
        else:
            print("{} 文件夹已存在!".format(self.data_dir))

    def pull_data(self, code = "csi300", start_date="20090101", end_date="20250101"):
        if code == "csi300":
            data_df = self.akshare_data.get_ch_index_stock_daily(index_codes="000300", start_date=start_date, end_date=end_date)
        elif code == "sse50":
            data_df = self.akshare_data.get_ch_index_stock_daily(index_codes="000016", start_date=start_date, end_date=end_date)
        elif code == "all":
            data_df = self.akshare_data.get_ch_stock_daily_all(start_date=start_date, end_date=end_date)
        else:
            data_df = self.akshare_data.get_ch_stock_daily(symbol=code, start_date=start_date, end_date=end_date)

        if data_df is None:
            print("获取数据失败，请检查网络连接是否稳定或库版本")

        data_df.to_csv(os.path.join(self.data_dir, "data_"+str(code)+".csv"), index=False)


if __name__ == "__main__":
    start_date = "20090101"
    end_date = datetime.now().strftime("%Y%m%d")
    StockData().pull_data(code="all", start_date=start_date, end_date=end_date)
