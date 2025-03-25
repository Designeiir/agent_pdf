from agent_network.base import BaseAgent
from agent_network.exceptions import ReportError
import os
import agent_network.graph.context as ctx


# pdf提取表格内容
class pdf_extract_table_agent(BaseAgent):
    def __init__(self, graph, config, logger):
        super().__init__(graph, config, logger)

    def forward(self, messages, **kwargs):
        # 检查参数
        # pdf_file_name = kwargs.get("file_path")
        pdf_file_name = ctx.retrieves_all()["file_path"]

        import pdfplumber
        from tabulate import tabulate
        # 提取pdf表格
        pdf_table = ''
        with pdfplumber.open(pdf_file_name) as pdf:
            for page in pdf.pages:
                page_tables = page.extract_tables()
                for table in page_tables:
                    pdf_table = pdf_table + "\n\n" + tabulate(table, tablefmt="grid")

        self.log("assistant", pdf_table)

        # 接入大模型完成任务
        if error_message := kwargs.get("graph_error_message"):
            prompt = f"错误：{error_message}"
        else:
            task = kwargs.get("task")
            prompt = task
            prompt += f"\n\n相关文件的表格识别结果为 {pdf_table}"
        self.add_message("user", prompt, messages)
        response = self.chat_llm(messages,
                                 api_key="sk-ca3583e3026949299186dcbf3fc34f8c",
                                 base_url="https://api.deepseek.com",
                                 model="deepseek-chat",
                                 response_format={"type": "json_object"})

        response_data = response.content

        self.log("assistant", response_data)

        if "result" in response_data:
            result = {
                "pdf_table_result": response_data["result"]
            }
            return result
        else:
            raise ReportError("unknown response format", "worker")