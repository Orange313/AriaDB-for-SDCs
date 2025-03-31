

class BPlusNode:
    def __init__(self):
        self.parent = None
        self.is_leaf = False

    def is_full(self):
        raise NotImplementedError


class LeafNode(BPlusNode):
    def __init__(self, leaf_size):
        super().__init__()
        self.is_leaf = True
        self.leaf_size = leaf_size
        self.records = []
        self.next_leaf = None

    def is_full(self):
        return len(self.records) > self.leaf_size

    def insert_record(self, lsn, record):
        inserted = False
        for i, (existing_lsn, _) in enumerate(self.records):
            if lsn < existing_lsn:
                self.records.insert(i, (lsn, record))
                inserted = True
                break
        if not inserted:
            self.records.append((lsn, record))

    def split(self):
        mid = len(self.records) // 2
        new_leaf = LeafNode(self.leaf_size)
        new_leaf.records = self.records[mid:]

        # if self.records[-1][0] +1 != new_leaf.records[0][0]:
        #     print(f"LSN discontinuity:{self.records[-1][0]}->{new_leaf.records[0][0]}")
        self.records = self.records[:mid]
        new_leaf.next_leaf = self.next_leaf
        self.next_leaf = new_leaf
        new_leaf.parent = self.parent
        return new_leaf


class InternalNode(BPlusNode):
    def __init__(self, order, buffer_size):
        super().__init__()
        self.keys = []
        self.children = []
        self.order = order
        self.buffer_size = buffer_size
        self.buffer = []

    def is_full(self):
        return len(self.children) > self.order

    def insert_into_buffer(self, lsn, record):
        self.buffer.append((lsn, record))
        if len(self.buffer) >= self.buffer_size:
            self.flush_buffer()

    def flush_buffer(self):
        self.buffer.sort(key=lambda x: x[0])
        for lsn, rec in self.buffer:
            idx = self.find_child_index(lsn)
            child = self.children[idx]
            if child.is_leaf:
                child.insert_record(lsn, rec)
                if child.is_full():
                    self.split_child(child)
            else:
                child.insert_into_buffer(lsn, rec)
        self.buffer.clear()

    def find_child_index(self, lsn):
        # for i, key in enumerate(self.keys):
        #     if lsn < key:
        #         return i
        # return len(self.keys)
        left, right = 0, len(self.keys)
        while left < right:
            mid = (left + right) // 2
            if lsn < self.keys[mid]:
                right = mid
            else:
                left = mid + 1
        return left

    def split_child(self, child):
        if child.is_leaf:
            new_leaf = child.split()
            new_key = new_leaf.records[0][0]
            idx = self.children.index(child)
            self.children.insert(idx + 1, new_leaf)
            self.keys.insert(idx, new_key)
            if self.is_full():
                self.split_self()
        else:
            # new_node = child.split_internal()
            # push_up_key = child.keys[-1]
            # child.keys.pop()
            # idx = self.children.index(child)
            # self.children.insert(idx + 1, new_node)
            # self.keys.insert(idx, push_up_key)
            # if self.is_full():
            #     self.split_self()
            mid = len(child.keys)//2
            push_up_key = child.keys[mid]
            new_node = child.split_internal()
            idx = self.children.index(child)
            self.children.insert(idx + 1, new_node)
            self.keys.insert(idx, push_up_key)
            if self.is_full():
                self.split_self()

    # def split_self(self):
    #     if not self.parent:
    #         new_root = InternalNode(self.order, self.buffer_size)
    #         new_node = self.split_internal()
    #         push_up_key = self.keys.pop()
    #         new_root.keys = [push_up_key]
    #         new_root.children = [self, new_node]
    #         self.parent = new_root
    #         new_node.parent = new_root
    #         return new_root
          
    #     else:
    #         new_node = self.split_internal()
    #         push_up_key = self.keys.pop()
    #         parent = self.parent
    #         idx = parent.children.index(self)
    #         parent.children.insert(idx + 1, new_node)
    #         parent.keys.insert(idx, push_up_key)
    #         if parent.is_full():
    #             return parent.split_self()
    #         return self.parent
    def split_self(self):
        if not self.parent:
            mid = len(self.keys) // 2
            push_up_key = self.keys[mid]  # 保存中间键
            
            new_root = InternalNode(self.order, self.buffer_size)
            new_node = self.split_internal()  # 分裂当前节点
            
            new_root.keys = [push_up_key]
            new_root.children = [self, new_node]
            self.parent = new_root
            new_node.parent = new_root
            return new_root
        else:
            mid = len(self.keys) // 2
            push_up_key = self.keys[mid]  # 保存中间键
            
            new_node = self.split_internal()
            parent = self.parent
            
            idx = parent.children.index(self)
            parent.children.insert(idx + 1, new_node)
            parent.keys.insert(idx, push_up_key)
            
            if parent.is_full():
                return parent.split_self()
            return self.parent

    def split_internal(self):
        mid = len(self.keys) // 2
        new_node = InternalNode(self.order, self.buffer_size)
        new_node.parent = self.parent
        new_node.keys = self.keys[mid + 1:]
        self.keys = self.keys[:mid]
        new_node.children = self.children[mid + 1:]
        self.children = self.children[:mid + 1]
        for child in new_node.children:
            child.parent = new_node
        return new_node
