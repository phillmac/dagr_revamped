from .config import DagrDockerConfig
from sqlalchemy import create_engine

class DAGRDocker():
    def __init__(self):
        self.config = DagrDockerConfig()
        engine = create_engine(self.config.mysql_conn)

def main():
    app = DAGRDocker()

if __name__ == '__main__':
    main()

