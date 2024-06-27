import csv
import networkx as nx
import matplotlib.pyplot as plt

def read_hierarchy_from_csv(filename='customer_hierarchy.csv'):
    hierarchy = []
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            parent, child = row
            hierarchy.append((parent, child))
    return hierarchy

def build_graph(hierarchy):
    G = nx.DiGraph()  # Create a directed graph
    G.add_edges_from(hierarchy)
    return G

def save_hierarchy_as_png(G, output_file='customer_hierarchy.png'):
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=3000, node_color='lightblue', font_size=10, font_weight='bold', arrowsize=20)
    plt.title('Customer Hierarchy')
    plt.savefig(output_file)
    plt.close()

def main():
    hierarchy = read_hierarchy_from_csv()
    G = build_graph(hierarchy)
    save_hierarchy_as_png(G)

if __name__ == "__main__":
    main()
