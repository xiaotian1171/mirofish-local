"""自建（本地）Zep 后端端到端自检。

在 /workspaces/mirofish/backend 下执行：
    python scripts/local_zep_smoke.py
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import Config  # noqa: E402

assert Config.ZEP_BACKEND == "local", f"ZEP_BACKEND={Config.ZEP_BACKEND}，请设成 local"
assert Config.validate() == [], f"配置校验未通过：{Config.validate()}"

from app.utils.zep import get_zep_client  # noqa: E402
from app.utils.zep_paging import fetch_all_edges, fetch_all_nodes  # noqa: E402

TEXT_1 = """星海智算是一家位于杭州的人工智能公司，2023 年由李明创立。
公司主要产品是「深海一号」推理芯片，主要客户包括云栖科技和江南银行。
2025 年星海智算与云栖科技签署了为期三年的算力合作协议。"""

TEXT_2 = """云栖科技总部在深圳，主营云计算服务。云栖科技的首席技术官是王芳。
王芳同时担任星海智算的技术顾问，负责「深海一号」的架构评审。"""


def wait_episode(client, episode_uuid: str, timeout: float = 180.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        episode = client.graph.episode.get(uuid_=episode_uuid)
        if getattr(episode, "processed", False):
            return True
        time.sleep(2)
    return False


def main() -> int:
    client = get_zep_client()
    graph_id = f"smoke_{int(time.time())}"
    print(f"[1] 创建图谱 {graph_id}")
    client.graph.create(graph_id=graph_id, name="smoke graph")
    assert client.graph.get(graph_id).graph_id == graph_id

    print("[2] 写入 episode（graph.add）")
    episode = client.graph.add(
        graph_id=graph_id,
        type="text",
        data=TEXT_1,
        source_description="smoke test",
        metadata={"source": "smoke"},
    )
    assert episode.uuid_, "episode 缺少 uuid_"
    started = time.time()
    assert wait_episode(client, episode.uuid_), "episode 抽取超时"
    print(f"    抽取完成，用时 {time.time() - started:.1f}s")

    print("[3] 分页读取节点 / 边（with_raw_response）")
    nodes = fetch_all_nodes(client, graph_id, page_size=2)
    edges = fetch_all_edges(client, graph_id, page_size=2)
    print(f"    nodes={len(nodes)} edges={len(edges)}")
    assert nodes, "未抽取出任何实体"
    assert edges, "未抽取出任何关系"
    assert all(getattr(node, "name", None) for node in nodes), "节点缺少 name"
    assert all(getattr(node, "labels", None) for node in nodes), "节点缺少 labels"
    assert all(getattr(edge, "fact", None) for edge in edges), "边缺少 fact"
    assert all(getattr(edge, "source_node_uuid", None) for edge in edges), "边缺少 source_node_uuid"

    print("[4] 单点读取 node.get / node.get_edges")
    first_node = nodes[0]
    assert client.graph.node.get(uuid_=first_node.uuid_).name == first_node.name
    assert client.graph.node.get_edges(node_uuid=first_node.uuid_)

    print("[5] graph.search")
    results = client.graph.search(
        graph_id=graph_id,
        query="星海智算 与 云栖科技 的合作关系",
        limit=5,
        scope="edges",
        reranker="cross_encoder",
    )
    assert results.edges, "检索未返回边"
    print(f"    hit edges={len(results.edges)} fact[0]={results.edges[0].fact[:60]}")

    print("[6] batch.create / add / process / get / list_items")
    batch = client.batch.create(
        metadata={"graph_id": graph_id, "mirofish_operation_id": "smoke-op", "chunk_count": 1}
    )
    items = client.batch.add(batch_id=batch.batch_id, items=[{"type": "text", "data": TEXT_2, "metadata": {}}])
    assert len(items) == 1 and items[0].episode_uuid
    client.batch.process(batch_id=batch.batch_id)
    deadline = time.time() + 300
    status = None
    while time.time() < deadline:
        summary = client.batch.get(batch_id=batch.batch_id)
        status = summary.status
        if status in {"succeeded", "partial", "failed", "invalid", "canceled"}:
            break
        time.sleep(3)
    print(f"    batch 状态={status} 进度={client.batch.get(batch_id=batch.batch_id).progress.percent_complete}%")
    assert status == "succeeded", f"batch 未成功：{status}"
    batch_items = client.batch.list_items(batch_id=batch.batch_id)
    assert batch_items.items and batch_items.items[0].status == "succeeded"
    assert client.batch.list().batches, "batch.list 未返回批次"

    print("[7] 二次抽取合并实体")
    nodes_after = fetch_all_nodes(client, graph_id, page_size=50)
    edges_after = fetch_all_edges(client, graph_id, page_size=50)
    print(f"    nodes={len(nodes_after)} edges={len(edges_after)}")
    assert len(nodes_after) >= len(nodes), "第二次抽取后节点数反而减少"

    print("[8] 清理图谱")
    client.graph.delete(graph_id=graph_id)
    try:
        client.graph.get(graph_id)
        raise AssertionError("删除后仍能读到图谱")
    except Exception as error:  # noqa: BLE001
        print(f"    删除校验通过（{type(error).__name__}）")

    print("\n✅ 自建 Zep 后端全部自检通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
