
from typing import List

class BucketBuilder:

    def __init__(self):
        self.bucket = []

    # Add item to the bucket
    def add_item(self, item):
        if not isinstance(item, int):
            raise TypeError(f"Expected int, got {type(item).__name__}")
        self.bucket.append(item)
        return self # For Method chainning

    # check if the bucket is empty
    def is_empty(self):
        return len(self.bucket) == 0

    def get_items(self) -> List[int]:
        #Return a copy of the bucket contents.
        return self.bucket.copy()

    # Remove item from the bucket
    def remove_item(self, item):
        if item in self.bucket:
            self.bucket.remove(item)
        return self

    # Display the content of the bucket
    def display_items(self):
        for i in self.bucket:
            print(i)



def process_bucket(max_items: int = 10,
                   adding: bool = True,
                   item_to_remove: Optional[int] = None) -> List[int]:

    b =BucketBuilder()

    if adding:
        for item_index in range(max_items):
            b.add_item(item_index)
    elif item_to_remove is not None:
        b.remove_item(item_to_remove)

    return b.get_items()
#
# if __name__ == '__main__':
#     # Create bucket and add items
#     b = BucketBuilder()
#     for i in range(10):
#         b.add_item(i)
#
#     print(f"Bucket contents: {b.get_items()}")
#
#     # Remove first item
#     if len(b.bucket) > 0:
#         item_to_remove = b.get_items()[0]
#         b.remove_item(item_to_remove)
#
#     print(f"After removal: {b.get_items()}")



if __name__ == '__main__':
    # Demonstrate usage
    bucket_contents = process_bucket(adding=True)
    print(f"Bucket contents: {bucket_contents}")

    # Remove specific item (without recursion)
    if bucket_contents:
        item_to_remove = bucket_contents[0]
        bucket_contents = process_bucket(adding=False, item_to_remove=item_to_remove)
        print(f"After removal: {bucket_contents}")
        print("Good Bye have a nice day")


