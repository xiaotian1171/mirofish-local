"""通过真实 HTTP 接口验证 MiroFish 端到端链路（本地 Zep 后端）。

在 code50 机上执行：
    python scripts/e2e_graph_build.py
"""

from __future__ import annotations

import json
import time

import requests

BASE = "http://127.0.0.1:5001/api"

NEWS_TEXT = """星海智算与云栖科技算力合作引发行业关注

2026 年 3 月，杭州的 AI 芯片公司星海智算宣布与深圳云计算厂商云栖科技达成三年期战略合作，
云栖科技将采购不少于 30 万片「深海一号」推理芯片，用于其新一代智算中心。
星海智算创始人李明在发布会上表示，本次合作的总金额约为 45 亿元。

业内分析认为，这次合作会挤压江南银行等金融机构自建算力团队的采购预算。
江南银行首席技术官王芳此前主导过自研算力集群项目，该项目在 2025 年因成本问题暂停。
部分分析师担心，星海智算的产能爬坡速度可能无法支撑如此大的订单量。
与此同时，云栖科技的主要竞争对手南山智算正在加快自建芯片团队，并在苏州设立了研发中心。

监管层面，工信部在 2026 年初发布了智算中心建设指引，要求新增算力项目披露能耗与国产化率。
云栖科技回应称将完全符合新规，并计划在 2026 年底前完成首批交付。
市场对星海智算的估值预期因此上调，多家投资机构将其列入重点关注名单。"""


def main() -> int:
    requirement = "预测这次算力合作对 AI 芯片行业与金融机构自建算力的影响，并给出不同角色的观点"

    files = {"files": ("mirofish_demo_news.txt", NEWS_TEXT.encode("utf-8"), "text/plain")}
    form = {"simulation_requirement": requirement, "project_name": "算力合作影响模拟"}
    response = requests.post(f"{BASE}/graph/ontology/generate", files=files, data=form, timeout=600)
    print("[1] ontology/generate ->", response.status_code)
    payload = response.json()
    if not payload.get("success"):
        print("    失败：", json.dumps(payload, ensure_ascii=False)[:500])
        return 1
    data = payload["data"]
    project_id = data["project_id"]
    ontology = data.get("ontology") or {}
    print(f"    project_id={project_id} 文本长度={data.get('total_text_length')}")
    print(f"    实体类型={[item.get('name') for item in ontology.get('entity_types', [])]}")
    print(f"    关系类型={[item.get('name') for item in ontology.get('edge_types', [])]}")

    started = time.time()
    response = requests.post(f"{BASE}/graph/build", json={"project_id": project_id}, timeout=120)
    print("[2] graph/build ->", response.status_code, json.dumps(response.json(), ensure_ascii=False)[:200])
    build = response.json()
    if not build.get("success"):
        return 1
    task_id = build["data"]["task_id"]

    graph_id = None
    while time.time() - started < 1500:
        task = requests.get(f"{BASE}/graph/task/{task_id}", timeout=60).json()
        info = task.get("data") or {}
        print(f"    [{time.time()-started:6.1f}s] status={info.get('status')} progress={info.get('progress')} msg={str(info.get('message'))[:70]}")
        graph_id = info.get("result", {}).get("graph_id") if info.get("status") == "completed" else graph_id
        if info.get("status") in {"completed", "failed"}:
            if info.get("status") == "failed":
                print("    构建失败：", str(info.get("error"))[:800])
                return 1
            print("    graph_info =", json.dumps(info.get("result", {}).get("graph_info"), ensure_ascii=False))
            break
        time.sleep(8)

    print("[3] graph/data 校验")
    graph_data = requests.get(f"{BASE}/graph/data/{graph_id}", timeout=120).json()
    node_count = len(((graph_data.get("data") or {}).get("nodes")) or [])
    edge_count = len(((graph_data.get("data") or {}).get("edges")) or [])
    print(f"    nodes={node_count} edges={edge_count}")
    assert node_count > 0 and edge_count > 0, "图谱为空"
    print(f"\n✅ 端到端构建通过，用时 {time.time()-started:.1f}s，project_id={project_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
