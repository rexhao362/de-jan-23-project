class BaseTableDataSource:
    def __init__(self, type=None, path=None):
        self.type = type
        self.path = path

    def is_initialized(self):
        return self.type != None

class DataFromPyArrowTable(BaseTableDataSource):
    def __init__(self):
        super().__init__("pyarrow_table")

class DataFromParquetFile(BaseTableDataSource):
    def __init__(self, path):
        super().__init__("parquet_file", path)