"""Task dependency graph and topological ordering."""

from graphlib import TopologicalSorter
from .models import Pipeline


def build_sorter(pipeline: Pipeline):
    """Build a topological sorter from a pipeline's task dependency graph.

    Validates that all dependency references point to existing tasks and
    returns a configured TopologicalSorter ready for execution.

    Args:
        pipeline: The Pipeline whose tasks define the dependency graph.

    Returns:
        A TopologicalSorter instance in prepared state.

    Raises:
        ValueError: If any task depends on a non-existent task name.
    """
    graph = {}
    for t in pipeline.tasks:
        graph[t.name] = set(t.depends_on)
    all_names = set(graph.keys())
    for t in pipeline.tasks:
        for dep in t.depends_on:
            if dep not in all_names:
                raise ValueError(f"Task {t.name} depends on non-existent task {dep}")
    return TopologicalSorter(graph)