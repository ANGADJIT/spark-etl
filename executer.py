from src.extracters.extracter import Extracter
from src.transformers.transformers import Transformers
from src.loaders.loader import Loader

if __name__ == '__main__':
    extracter: Extracter = Extracter()
    transformer: Transformers = Transformers(extracter=extracter,master='yarn')
    loader: Loader = Loader(transformer=transformer)

    loader.load()

    del extracter
