from graphlib import TopologicalSorter
from .models import Pipeline

def build_sorter(pipeline: Pipeline) -> TopologicalSorter:
    graph = {}
    for t in pipeline.tasks:
        graph[t.name] = set(t.depends_on)
    all_names = set(graph.keys())
    for t in pipeline.tasks:
        for dep in t.depends_on:
            if dep not in all_names:
                raise ValueError(f"Task {t.name} depends on non-existent task {dep}")
    return TopologicalSorter(graph)