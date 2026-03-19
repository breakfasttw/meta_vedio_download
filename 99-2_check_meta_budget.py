from metacontentlibraryapi import MetaContentLibraryAPIClient

# 初始化 Client
client = MetaContentLibraryAPIClient()
client.set_default_version(client.LATEST_VERSION)

# 呼叫預算端點
response = client.get(path="budgets")
budget_info = response.json()

# 查看多媒體預算使用狀況
multimedia_usage = budget_info.get('multimedia', {})
print(f"目前多媒體已使用額度: {multimedia_usage.get('total_usage')}")
print(f"多媒體最大額度限制: {multimedia_usage.get('max_usage_limit')}")