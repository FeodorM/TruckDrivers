#!/usr/bin/env python3


class Graph:
    def __init__(self, matrix):
        assert len(matrix) == len(matrix[0]), "Матрица должна быть квадратной"

        self.matrix = [[int(bool(x)) for x in row] for row in matrix]

        for i in range(len(matrix)):
            self.matrix[i][i] = 1
            for j in range(i):
                self.matrix[j][i] = self.matrix[i][j]

        self._adj_levels = None

    @staticmethod
    def from_file(filename):
        with open(filename) as f:
            return Graph([[int(x) for x in row.strip().split()] for row in f])

    def __str__(self):
        head = ' \t' + ' '.join(str(x + 1) for x in range(len(self.matrix))) + '\n\n'
        column_number = lambda x: str(x + 1) + '\t'
        return head + '\n'.join(
            column_number(i) +
            ' '.join(str(x) for x in row) for i, row in enumerate(self.matrix)
        )

    def adj(self, w):
        adj = set()
        for i in w:
            adj |= {j for j, x in enumerate(self.matrix[i]) if x and j != i}
        return [x for x in adj if x not in w]

    @property
    def adjacency_levels(self):
        if self._adj_levels is not None:
            return self._adj_levels

        unmarked = list(range(1, len(self.matrix)))
        levels = adj = [[0]]

        while unmarked and adj:
            adj = [x for x in self.adj(levels[-1]) if x in unmarked]
            levels.append(adj)
            unmarked = [x for x in unmarked if x not in adj]

        self._adj_levels = [[x + 1 for x in level] for level in levels]
        return self._adj_levels

    @property
    def adjance_levels_length(self):
        return len(self.adjacency_levels) - 1

    @property
    def adjacency_levels_width(self):
        return len(max(self.adjacency_levels, key=len))


if __name__ == '__main__':
    # g = Graph([
    #     [0, 0, 0],
    #     [1, 1, 0],
    #     [1, 0, 0]
    # ])
    g = Graph.from_file('graph')
    print(g)
    print('\nУровни смежности:', *g.adjacency_levels, '', sep='\n')
    print('Длина и ширина СУС:', g.adjance_levels_length, g.adjacency_levels_width)
