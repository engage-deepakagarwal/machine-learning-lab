from datetime import datetime
from tqdm import tqdm
from datasets import load_dataset
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from items import Item

CHUNK_SIZE = 1000
MIN_PRICE = 0.5
MAX_PRICE = 999.49


class ItemLoader:
    def __init__(self, name):
        """
        Initialize the ItemLoader with a dataset name.

        Args:
            name (str): Name of the dataset to load.
        """
        self.name = name
        self.dataset = None

    def from_datapoint(self, datapoint):
        """
        Try to create an Item from this datapoint.

        Args:
            datapoint (dict): A single data point from the dataset.

        Returns:
            Item or None: The created Item if successful, or None if it shouldn't be included.
        """
        try:
            price_str = datapoint['price']
            if price_str:
                price = float(price_str)
                if MIN_PRICE <= price <= MAX_PRICE:
                    item = Item(datapoint, price)
                    return item if item.include else None
        except ValueError:
            return None

    def from_chunk(self, chunk):
        """
        Create a list of Items from a chunk of dataset elements.

        Args:
            chunk (list): A list of data points from the dataset.

        Returns:
            list: List of Item objects created from the chunk.
        """
        return [result for result in map(self.from_datapoint, chunk) if result]

    def chunk_generator(self):
        """
        Generate chunks of datapoints from the dataset.

        Yields:
            Dataset: A chunk of datapoints from the dataset.
        """
        size = len(self.dataset)
        for i in range(0, size, CHUNK_SIZE):
            yield self.dataset.select(range(i, min(i + CHUNK_SIZE, size)))

    def load_in_parallel(self, workers):
        """
        Load and process the dataset in parallel using multiple workers.

        Args:
            workers (int): Number of worker processes to use.

        Returns:
            list: List of processed Item objects.
        """
        results = []
        chunk_count = (len(self.dataset) // CHUNK_SIZE) + 1

        with ProcessPoolExecutor(max_workers=workers) as pool:
            for batch in tqdm(
                pool.map(self.from_chunk, self.chunk_generator()),
                total=chunk_count,
                desc=f"Processing {self.name}"
            ):
                results.extend(batch)

        for result in results:
            result.category = self.name

        return results

    def load(self, workers=8):
        """
        Load and process the dataset using parallel processing.

        Args:
            workers (int, optional): Number of worker processes. Defaults to 8.

        Returns:
            list: List of processed Item objects.
        """
        start = datetime.now()
        print(f"Loading dataset {self.name}", flush=True)

        self.dataset = load_dataset(
            "McAuley-Lab/Amazon-Reviews-2023",
            f"raw_meta_{self.name}",
            split="full",
            trust_remote_code=True
        )

        results = self.load_in_parallel(workers)
        finish = datetime.now()

        print(
            f"Completed {self.name} with {len(results):,} datapoints in {(finish-start).total_seconds()/60:.1f} mins",
            flush=True
        )

        return results
