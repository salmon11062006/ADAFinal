class Graph:
    def __init__(self, size):
        self.adj_matrix = [[0] * size for _ in range(size)]