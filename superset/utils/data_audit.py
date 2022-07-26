import json
import logging
import requests
import urllib.parse
from json import JSONDecodeError
from superset import config

logger = logging.getLogger(__name__)

FILTER_PATH = ["login", "static"]

CONTINUE_PATH = ["api", "v1", "superset"]

PATH_TRANS_DESC = {
    "log": "日志",
    "explore_json": "处理form_data数据",
    "sql_json": "处理sql数据",
    "time_range": "时间范围",
    "explore": "处理数据",
    "report": "报告",
    "favstar": {
        "default": "",
        "Dashboard": {
            "default": "看板",
            "count": "收藏状态"
        },
    },
    "dashboard": {
        "default": "看板",
        "_info": "信息",
        "export": "导出",
        "favorite_status": "收藏状态",
        "import": "导入",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "created_by": "创建人",
        },
        "charts": "包涵的图表",
        "datasets": "包涵的数据集",
    },
    "chart": {
        "default": "图表",
        "default_level": 2,
        "_info": "信息",
        "data": "数据",
        "export": "导出",
        "favorite_status": "收藏状态",
        "import": "导入",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "created_by": "创建人",
        }
    },
    "database": {
        "default": "数据库",
        "_info": "信息",
        "available": "当前可用数据库的名称",
        "export": "导出",
        "import": "导入",
        "test_connetcion": "连接测试",
        "validate_parameters": "连接参数验证",
        "function_names": "支持的函数名",
        "related_objects": "关联的图表或者看板",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "created_by": "创建人",
        },
        "schemas": "视图",
        "select_star": "星选表或者字段",
        "table": "表",
    },
    "dataset": {
        "default": "数据集",
        "distinct": "去重",
        "export": "导出",
        "import": "导入",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "database": "数据库",
        },
        "column": " 字段",
        "metric": " 配置",
        "refresh": "刷新",
        "related_objects": "关联的图表或者看板",

    },
    "tables": "数据表",
    "query": {
        "default": "查询",
        "distinct": "去重",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "database": "数据库",
        },
    },
    "saved_query": {
        "default": "已保存查询",
        "_info": "信息",
        "distinct": "去重",
        "export": "导出",
        "import": "导入",
        "related": {
            "default": "关联的",
            "owners": "拥有者",
            "database": "数据库",
        },
    },
    "datasource": {
        "default": "数据集",
        "save": "保存",
    },
    "csstemplateasyncmodelview": {
        "default": "css模板视图",
        "read": "加载",
    },
    "welcome": {
        "default": "登录",
    },
    "dashboardasync": {
        "default": "看板",
        "read": "异步加载",
    },
}


class DataAudit():

    def __init__(self, request) -> None:
        self.environ = request.environ
        self.url = config.AUDIT_URL

    def _build_desc(self, method, path):

        if method == "DELETE":
            operate = "删除"
        elif method == "GET":
            operate = "获取"
        elif method == "PUT ":
            operate = "更新"
        elif method == "POST":
            operate = "提交"
        else:
            operate = "操作"

        desc = ""
        default_content = ""
        content_dic = PATH_TRANS_DESC
        path_list = path.split("/")
        for i in path_list:
            if not i or i in CONTINUE_PATH:
                continue
            if str(i).isdigit():
                desc += f"id为{i}"
                continue
            elif not content_dic:
                break
            default_content = content_dic.get("default", "")
            content = content_dic.get(i, default_content)
            if isinstance(content, str):
                desc += content
                content_dic = {}
            elif isinstance(content, dict):
                desc += content.get("default", "")
                content_dic = content
        desc = operate+desc if desc else "未定义"
        return desc

    def _build_data(self, username):
        data = None
        path = self.environ["PATH_INFO"]
        if path.split("/")[1] not in FILTER_PATH:
            request = self.environ["werkzeug.request"]
            query = urllib.parse.unquote(request.query_string.decode())
            method = request.method
            body = request.form or request.data.decode()
            body = self._formate_data(body, trans=True)
            referer = self.environ.get('HTTP_REFERER', '')
            data = {
                "user": username,
                "platform": "ddbi",
                "desc": self._build_desc(method, path),
                "details": json.dumps({"method": method, "path": path, "query": query, "body": body, "referer": referer})
            }
        return data

    def _formate_data(self, data, trans=False):
        if isinstance(data, dict) and trans:
            return "&".join([f"{key}={self._formate_data(value)}" for key, value in data.items()])
        elif isinstance(data, (list, tuple, set)):
            return [self._formate_data(value) for value in data]
        elif isinstance(data, str):
            try:
                return self._formate_data(json.loads(data), trans=trans)
            except JSONDecodeError:
                return data
        else:
            return data

    def push_to_data_audit(self, username):
        data = self._build_data(username)
        if data:
            try:
                requests.post(self.url, json=data)
            except Exception as e:
                logger.error(f"SEND DATA-AUDIT {e}")
