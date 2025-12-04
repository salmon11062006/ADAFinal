class Graph:
    def __init__(self, size):
        self.adj_matrix = [[0] * size for _ in range(size)]
        self.size = size
        self.vertex_data = [''] * size

        def add_edge(self, u, v, capacity):
            self.adj_matrix[u][v] = capacity

        def add_vertex_data(self, vertex, data):
            if 0 <= vertex < self.size:
                self.vertex_data[vertex] = data

        def bfs(self, source, sink, parent):
            visited = [False] * self.size
            queue = []
            queue.append(source)
            visited[source] = True

            while queue:
                u  = queue.pop(0)

                for ind, val in enumerate(self.adj_matrix[u]):
                    if not visited[ind] and val > 0:
                        queue.append(ind)
                        visited[ind] = True
                        parent[ind] = u

            return visited[sink]
    
    def edmonds_karp(self, source, sink):
        parent = [-1] * self.size
        max_flow = 0

        while self.bfs(source, sink, parent):
            path_flow = float('Inf')
            s = sink

            while s != source:
                path_flow = min(path_flow, self.adj_matrix[parent[s]][s])
                s = parent[s]

            max_flow += path_flow
            v = sink
            while v != source:
                u = parent[v]
                self.adj_matrix[u][v] -= path_flow
                self.adj_matrix[v][u] += path_flow
                v = parent[v]

            path = []
            v = sink
            while v != source:
                path.append(v)
                v = parent[v]
            path.append(source)
            path.reverse()
            path_names = [self.vertex_data[node] for node in path]
            print ("path: ", " -> ".join(path_names), ", flow: ", path_flow)

        return max_flow
    
'''
# Example usage:

g = Graph(6)
vertex_names = ['s', 'v1', 'v2', 'v3', 'v4', 't']
for i, name in enumerate(vertex_names):
    g.add_vertex_data(i, name)

g.add_edge(0, 1, 3)  # s  -> v1, cap: 3
g.add_edge(0, 2, 7)  # s  -> v2, cap: 7
g.add_edge(1, 3, 3)  # v1 -> v3, cap: 3
g.add_edge(1, 4, 4)  # v1 -> v4, cap: 4
g.add_edge(2, 1, 5)  # v2 -> v1, cap: 5
g.add_edge(2, 4, 3)  # v2 -> v4, cap: 3
g.add_edge(3, 4, 3)  # v3 -> v4, cap: 3
g.add_edge(3, 5, 2)  # v3 -> t,  cap: 2
g.add_edge(4, 5, 6)  # v4 -> t,  cap: 6

source = 0
sink = 5

print("The maximum possible flow is:", g.edmonds_karp(source, sink))
'''