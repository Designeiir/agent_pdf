from agent_network.base import BaseAgent
from agent_network.exceptions import ReportError
import os
import agent_network.utils.storage.oss as oss
import requests
import time
import pdfplumber
import pandas as pd


def get_upload_url(file_path):
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise ReportError('File does not exist')
    # 提取文件名
    file_name = os.path.basename(file_path).replace('.pdf', '')
    # 添加时间信息
    file_url = time.strftime("%Y%m%d%H%M%S") + file_name
    # 返回
    return file_url


# pdf提取表格内容
class pdf_extract_table_agent(BaseAgent):
    def __init__(self, graph, config, logger):
        super().__init__(graph, config, logger)

    def forward(self, messages, **kwargs):
        # 检查参数
        file_path = kwargs.get("file_path")
        if not file_path:
            raise ReportError("pdf_path is not provided", "AgentNetworkPlannerGroup/AgentNetworkPlanner")
        if not os.path.exists(file_path):
            raise ReportError("file not found", "AgentNetworkPlannerGroup/AgentNetworkPlanner")

        # 提取pdf表格
        pdf_table = list()
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_tables = page.extract_tables()
                # 将表格转为字符串格式
                for table in page_tables:
                    pdf_table.extend(table)

        # 建立csv文件名
        csv_file_name = get_upload_url(file_path) + "_table.csv"
        # 将表格信息写入暂存文件中
        table_csv = pd.DataFrame(data=pdf_table, index=None)
        table_csv_stream = table_csv.to_csv(index=False)

        # 将文件上传到oss
        oss.bucket.put_object(csv_file_name, table_csv_stream)
        url = oss.bucket.sign_url('GET', csv_file_name, 604800, slash_safe=True)
        print(url)
        oss.upload_file(url, requests.get(url).content)

        self.log("assistant", url)

        # 返回
        result = {
            "result": url
        }
        return result


class pdf_extract_text_agent(BaseAgent):
    def __init__(self, graph, config, logger):
        super().__init__(graph, config, logger)

    def forward(self, messages, **kwargs):
        # 检查参数
        file_path = kwargs.get("file_path")
        if not file_path:
            raise ReportError("pdf_path is not provided", "AgentNetworkPlannerGroup/AgentNetworkPlanner")
        if not os.path.exists(file_path):
            raise ReportError("file not found", "AgentNetworkPlannerGroup/AgentNetworkPlanner")

        # 提取文本
        pdf_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                pdf_text = pdf_text + "\n" + page.extract_text()
        self.log("assistant", pdf_text)

        # 建立text文件名
        txt_file_name = get_upload_url(file_path) + "_text.txt"
        # 上传到oss上
        oss.bucket.put_object(txt_file_name, pdf_text)
        url = oss.bucket.sign_url('GET', txt_file_name, 604800, slash_safe=True)
        print(url)
        oss.upload_file(url, requests.get(url).content)

        self.log("assistant", url)

        # 返回
        result = {
            "result": url
        }
        return result