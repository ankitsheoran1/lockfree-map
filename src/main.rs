use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::{ptr, thread};
use std::sync::{ Arc, RwLock };
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const REFRESH_RATE: usize = 1000;

#[derive(Debug)]
struct Node<K, V> {
    key: Option<K>,
    val: AtomicPtr<V>,
    next: AtomicPtr<Node<K, V>>,
}

#[derive(Debug)]
struct LinkedList<K, V> {
    head: AtomicPtr<Node<K, V>>,
    tail: AtomicPtr<Node<K, V>>,
}


impl<K, V> Node<K, V> {
    fn empty() -> Self {
       Node { key: None,
        val: AtomicPtr::new(ptr::null_mut()),
        next: AtomicPtr::new(ptr::null_mut()),
    }
  }

  fn new(key: K, val: V) -> Self {
    Node {
        key: Some(key),
        val: AtomicPtr::new(Box::into_raw(Box::new(val))),
        next: AtomicPtr::new(ptr::null_mut()),
    }
  }
}


impl<K,V> Default for  LinkedList<K, V> {
  fn default() -> Self {
      let head = Box::new(Node::empty());
      let tail = Box::into_raw(Box::new(Node::empty()));
      head.next.store(tail, Ordering::SeqCst);

      LinkedList {
        head: AtomicPtr::new(Box::into_raw(head)),
        tail: AtomicPtr::new(tail),
      }
  }
}


impl<K, V> LinkedList<K, V> 
where 
K: Ord,
V: Copy, {

    fn delete(&self, key: &K, remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<V>  {
        let mut left_node = ptr::null_mut();
        let mut right_node;
        let mut right_node_next;
        loop {
           right_node = self.search(key, &mut left_node, remove_nodes);
           if right_node == self.tail.load(Ordering::SeqCst) || 
             unsafe { &*right_node }.
                   key
                   .as_ref()
                   .map(|k| k != key)
                   .unwrap_or(true) {
                    return None; 
                 }
            right_node_next = unsafe { &*right_node }.next.load(Ordering::SeqCst); 
            if !Self::is_marked_reference(right_node_next) 
               && unsafe { &*right_node }.next.compare_and_swap(right_node_next, Self::get_marked_reference(right_node_next), Ordering::SeqCst) == right_node_next 
               {
                break;
               }    
        }

        let node = unsafe { &*right_node };
        let old_val = unsafe { *node.val.load(Ordering::SeqCst) };
        if unsafe { &*left_node }
        .next
        .compare_and_swap(right_node, right_node_next, Ordering::SeqCst)
        != right_node
    {
        let _ = self.search(
            unsafe { &*right_node }.key.as_ref().unwrap(),
            &mut left_node,
            remove_nodes,
        );
    } else {
        remove_nodes.push(right_node);
    }

    Some(old_val)

   } 

   fn insert(&self, key: K, val: V, remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<*mut V> {
       let mut new_node = Box::new(Node::new(key, val));
       let mut left_node = ptr::null_mut();

       loop {
         
         let right_node = self.search(new_node.key.as_ref().unwrap(), &mut left_node, remove_nodes);
         if right_node != self.tail.load(Ordering::SeqCst) &&
         unsafe { &*right_node }.key
                 .as_ref()
                 .map(|k| k == new_node.key.as_ref().unwrap())
                 .unwrap_or(false) {
                    let node = unsafe { &*right_node };
                    let value = Box::new(val);
                    let old = node.val.swap(Box::into_raw(value), Ordering::SeqCst);
                    remove_nodes.push(Box::into_raw(new_node));
                    return Some(old);

                 }

                 new_node.next.store(right_node, Ordering::SeqCst);
                 let new_node_ptr = Box::into_raw(new_node);
                 if unsafe { &*left_node }.next
                     .compare_and_swap(right_node, new_node_ptr, Ordering::SeqCst) == right_node {
                       return  None;
                     }
                     new_node = unsafe { Box::from_raw(new_node_ptr) };
            }
    }

    fn get(&self, key: &K,  remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<V> {
        let mut left_node = ptr::null_mut();
        let right_node = self.search(key, &mut left_node, remove_nodes);
        if right_node == self.tail.load(Ordering::SeqCst) 
           || unsafe { &*right_node }.key
              .as_ref()
              .map(|k| k != key )
              .unwrap_or(true) {
                return None;
              }

              unsafe { Some(*(&*right_node).val.load(Ordering::SeqCst)) }

    }

    fn is_marked_reference(ptr: *mut Node<K, V>) -> bool {
        (ptr as usize & 0x1) == 1
    }
    fn get_marked_reference(ptr: *mut Node<K, V>) -> *mut Node<K, V> {
        (ptr as usize | 0x1) as *mut _
    }
    fn get_unmarked_reference(ptr: *mut Node<K, V>) -> *mut Node<K, V> {
        (ptr as usize & !0x1) as *mut _
    }

    fn search(&self, search_key: &K, left_node: &mut *mut Node<K, V>, remove_nodes: &mut Vec<*mut Node<K, V>>) -> *mut Node<K, V> {
         let mut left_node_next = ptr::null_mut();
         let mut right_node;

         'search: loop {
            let mut t = self.head.load(Ordering::SeqCst);
            let mut t_next = unsafe {&*t}. next.load(Ordering::SeqCst);

            // Find left and right node 
            loop {
                if !Self::is_marked_reference(t_next) {
                    *left_node = t;
                    left_node_next = t_next;
                }
                // next iterate 
                if Self::is_marked_reference(t_next) {
                    t = Self::get_unmarked_reference(t_next);
                } else {
                    t = t_next;
                }

                if t == self.tail.load(Ordering::SeqCst) {
                    break;
                }
                t_next = unsafe { &*t }. next.load(Ordering::SeqCst);
                if !Self::is_marked_reference(t_next) && 
                 unsafe { &*t }.key
                         .as_ref()
                         .map( |k| k >= search_key)
                         .unwrap_or(false) {
                            break;
                         }

            }

            right_node = t;

            // if right and left nodes adjacent
            if left_node_next == right_node {
                if right_node != self.tail.load(Ordering::SeqCst) && 
                Self::is_marked_reference(unsafe { &*right_node }.next.load(Ordering::SeqCst)) {
                    continue 'search;
                } else {
                    return right_node;
                }

            }

            if unsafe { &**left_node }
                .next
                .compare_and_swap(left_node_next, right_node, Ordering::SeqCst)
                == left_node_next
            {
                let mut curr_node = left_node_next;

                loop {
                    assert_eq!(Self::is_marked_reference(curr_node), false);
                    remove_nodes.push(curr_node);
                    curr_node = unsafe { &*curr_node }.next.load(Ordering::SeqCst);
                    assert_eq!(Self::is_marked_reference(curr_node), true);
                    curr_node = Self::get_unmarked_reference(curr_node);
                    if curr_node == right_node {
                        break;
                    }
                }

                
                if right_node != self.tail.load(Ordering::SeqCst)    
                    && Self::is_marked_reference(unsafe { &*right_node }.next.load(Ordering::SeqCst))
                {
                    continue 'search;
                } else {
                    return right_node;
                }


            }
        }



    }
}

struct Table<K, V> {
    nbuckets: usize,
    map: Vec<LinkedList<K, V>>,
    nitems: AtomicUsize,
}

impl<K, V> Table<K, V> {
    fn new(nbuckets: usize) -> Self {
        let mut table = Table {
            nbuckets: nbuckets,
            map: Vec::with_capacity(nbuckets),
            nitems: AtomicUsize::new(0),
        };

        for _ in 0..nbuckets {
            table.map.push(LinkedList::default());
        }

        table
    }
}

impl<K, V> Table<K, V> 
where 
K: Hash + Ord ,
V: Copy + Debug,
{
    fn insert(&self, key: K, value: V, remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<*mut V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash: usize = hasher.finish() as usize;
        let idx = hash % self.nbuckets;
        let ret = self.map[idx].insert(key, value, remove_nodes);

        if ret.is_none() {
            self.nitems.fetch_add(1, Ordering::SeqCst);
        }

        ret

    }

    fn get(&self, key: &K, remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<V> {
        let mut hasher =  DefaultHasher::new();
        key.hash(&mut hasher);
        let hash: usize = hasher.finish() as usize;
        let idx = hash % self.nbuckets; 

        self.map[idx].get(key, remove_nodes)
    }

    fn delete(&self, key: &K, remove_nodes: &mut Vec<*mut Node<K, V>>) -> Option<V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash: usize = hasher.finish() as usize;
        let index = hash % self.nbuckets;
        let ret = self.map[index].delete(key, remove_nodes);

        if ret.is_some() {
            self.nitems.fetch_sub(1, Ordering::SeqCst);
        }

        ret

    }
}

struct Map<K, V> {
    table: Table<K, V>,
    handles: RwLock<Vec<Arc<AtomicUsize>>>
}



struct MapHandle<K, V> {
    map: Arc<Map<K, V>>,
    epoch_counter: Arc<AtomicUsize>,
    remove_nodes: Vec<*mut Node<K, V>>,
    remove_val: Vec<*mut V>,
    refresh: usize,
}

unsafe impl<K, V> Send for MapHandle<K, V>
where
    K: Send + Sync,
    V: Send + Debug,
{
}

impl<K, V> MapHandle<K, V> 
{
    fn cleanup(&mut self) {
        let mut started = Vec::new();
        let handles_map = self.map.handles.read().unwrap();
        for h in handles_map.iter() {
            started.push(h.load(Ordering::SeqCst));
        }

        for (i, h) in handles_map.iter().enumerate() {
            if started[i] % 2 == 0 {
                continue;
            }

            let mut check = h.load(Ordering::SeqCst);
            let mut iter = 0;
            while check <= started[i] && started[i] % 2 == 1 {
                if iter % 4 == 0 {
                    thread::yield_now();
                }
                check = h.load(Ordering::SeqCst);
                iter += 1;
            }
        }

        for to_drop in &self.remove_nodes {
            let val = unsafe { (&**to_drop).val.load(Ordering::SeqCst) };
            self.remove_val.push(val);

            drop(unsafe { Box::from_raw(*to_drop)});
        }

        for to_drop in &self.remove_val {
            drop(unsafe { Box::from_raw(*to_drop)});
        }

        self.remove_nodes = Vec::new();
        self.remove_val = Vec::new();

    } 
}

impl<K, V> MapHandle<K, V> 
where 
 K: Hash + Ord,
 V: Copy + Debug,
{
    fn insert(&mut self, key: K, val: V) -> Option<V> {
        self.refresh += 1;

        self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        let data = self.map.table.insert(key, val, &mut self.remove_nodes);
        self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        let mut ret = None;

        if let Some(v) = data {
            ret = Some(unsafe { *v });
            // drop(unsafe { Box::from_raw(v) });
            self.remove_val.push(v);
        }

        if self.refresh == REFRESH_RATE {
            self.refresh = 0;
            self.cleanup();
        }

        ret
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.refresh += 1;

        self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        let data = self.map.table.delete(key, &mut self.remove_nodes);
        self.epoch_counter.fetch_add(1, Ordering::SeqCst);

        if self.refresh == REFRESH_RATE {
            self.refresh = 0;
            self.cleanup();
        }

        data
     }

     fn get(&mut self, key: &K) -> Option<V> {
        self.refresh += 1;

        self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        let data = self.map.table.get(key, &mut self.remove_nodes);
        self.epoch_counter.fetch_add(1, Ordering::SeqCst);

        if self.refresh == REFRESH_RATE {
            self.refresh = 0;
            self.cleanup();
        }

        data
    }

    pub fn len(&self) -> usize {
        self.map.table.nitems.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.map.table.nitems.load(Ordering::SeqCst) == 0
    }

}

impl<K, V> Clone for MapHandle<K, V> {
    fn clone(&self) -> Self {
        let ret = Self {
            map: Arc::clone(&self.map),
            epoch_counter: Arc::new(AtomicUsize::new(0)),
            remove_nodes: Vec::new(),
            remove_val: Vec::new(),
            refresh: 0
        };

        let mut handles_vec = self.map.handles.write().unwrap();
        handles_vec.push(Arc::clone(&ret.epoch_counter));

        ret
    }
}

impl<K, V> Map<K, V> {

    pub fn with_capacity(nbuckets: usize) -> MapHandle<K, V> {
        let map = Map {
            table: Table::new(nbuckets),
            handles: RwLock::new(Vec::new()),
        };

        let ret = MapHandle {
            map: Arc::new(map),
            epoch_counter: Arc::new(AtomicUsize::new(0)),
            remove_nodes: Vec::new(),
            remove_val: Vec::new(),
            refresh: 0,
        };

        let hmap = Arc::clone(&ret.map);
        let mut handles_vec = hmap.handles.write().unwrap();
        handles_vec.push(Arc::clone(&ret.epoch_counter));

        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn linkedlist_basics() {
        let mut remove_nodes = Vec::new();

        let new_linked_list = LinkedList::default();

        println!("{:?}", new_linked_list);

        new_linked_list.insert(1, 1, &mut remove_nodes);
        new_linked_list.insert(2, 2, &mut remove_nodes);
        new_linked_list.insert(3, 3, &mut remove_nodes);
        new_linked_list.insert(4, 4, &mut remove_nodes);
        new_linked_list.insert(5, 5, &mut remove_nodes);
        new_linked_list.insert(6, 6, &mut remove_nodes);
        new_linked_list.insert(7, 7, &mut remove_nodes);
        new_linked_list.insert(8, 8, &mut remove_nodes);
        new_linked_list.insert(9, 9, &mut remove_nodes);
        new_linked_list.insert(10, 10, &mut remove_nodes);

        new_linked_list.delete(&6, &mut remove_nodes);

        println!("vector {:?}", remove_nodes);

        println!("printing list");
        // Print the entire linked list from head to tail
        let mut current_node = new_linked_list.head.load(Ordering::SeqCst);
        while current_node != new_linked_list.tail.load(Ordering::SeqCst) {
            println!("{:?}", unsafe { &*current_node });
            current_node = unsafe { &*current_node }.next.load(Ordering::SeqCst);
        }
    }

    #[test]
    fn more_linked_list_tests() {
        let mut remove_nodes = Vec::new();

        let new_linked_list = LinkedList::default();
        println!(
            "Insert: {:?}",
            new_linked_list.insert(5, 3, &mut remove_nodes)
        );
        println!(
            "Insert: {:?}",
            new_linked_list.insert(5, 8, &mut remove_nodes)
        );
        println!(
            "Insert: {:?}",
            new_linked_list.insert(2, 3, &mut remove_nodes)
        );

        println!("Get: {:?}", new_linked_list.get(&5, &mut remove_nodes));

        new_linked_list.delete(&5, &mut remove_nodes);
    }

    #[test]
    fn hasmap_curr() {
        let handle = Map::with_capacity(8);
        let mut threads = vec![];
        let nthreads = 10;
        for _ in 0..nthreads {
            let mut new_handle = handle.clone();
            threads.push(thread::spawn(move || {
                let n_iter = 1000000;
                for _ in 0..n_iter {
                    // let mut nhandle = handle.clone();
                    let mut rng = thread_rng();
                    let val = rng.gen_range(0..8);
                    let two = rng.gen_range(0..3);
                    if two % 3 == 0 {
                        new_handle.insert(val, val);
                    } else if two % 3 == 1 {
                        let v = new_handle.get(&val);
                        if v.is_some() {
                            assert_eq!(v.unwrap(), val);
                        }
                    } else {
                        new_handle.remove(&val);
                    }
                }
                assert_eq!(new_handle.epoch_counter.load(Ordering::SeqCst), n_iter * 2);
            }));
        }

                for t in threads {
                    t.join().unwrap();
                }
        }

        #[test]
        fn hashmap_remove() {
            let mut handle = Map::with_capacity(8);
            handle.insert(1, 3);
            handle.insert(2, 5);
            handle.insert(3, 8);
            handle.insert(4, 3);
            handle.insert(5, 4);
            handle.insert(6, 5);
            handle.insert(7, 3);
            handle.insert(8, 3);
            handle.insert(9, 3);
            handle.insert(10, 3);
            handle.insert(11, 3);
            handle.insert(12, 3);
            handle.insert(13, 3);
            handle.insert(14, 3);
            handle.insert(15, 3);
            handle.insert(16, 3);
            assert_eq!(handle.get(&1).unwrap(), 3);
            assert_eq!(handle.remove(&1).unwrap(), 3);
            assert_eq!(handle.get(&1), None);
            assert_eq!(handle.remove(&2).unwrap(), 5);
            assert_eq!(handle.remove(&16).unwrap(), 3);
            assert_eq!(handle.get(&16), None);
        }
    
        #[test]
        fn hashmap_basics() {
            let mut new_hashmap = Map::with_capacity(8);

            assert_eq!(new_hashmap.is_empty(), true);
            assert_eq!(new_hashmap.len(), 0);
    
            new_hashmap.insert(1, 1);
            new_hashmap.insert(2, 5);
            new_hashmap.insert(12, 5);
            new_hashmap.insert(13, 7);
            new_hashmap.insert(0, 0);
    
            new_hashmap.insert(20, 3);
            new_hashmap.insert(3, 2);
            new_hashmap.insert(4, 1);
    
            assert_eq!(new_hashmap.insert(20, 5).unwrap(), 3); //repeated
            assert_eq!(new_hashmap.insert(3, 8).unwrap(), 2); //repeated
            assert_eq!(new_hashmap.insert(5, 5), None);
    
            let cln = Arc::clone(&new_hashmap.map);
            assert_eq!(cln.table.nitems.load(Ordering::SeqCst), 9);
    
            new_hashmap.insert(3, 8); //repeated
    
            assert_eq!(new_hashmap.get(&20).unwrap(), 5);
            assert_eq!(new_hashmap.get(&12).unwrap(), 5);
            assert_eq!(new_hashmap.get(&1).unwrap(), 1);
            assert_eq!(new_hashmap.get(&0).unwrap(), 0);
            assert!(new_hashmap.get(&3).unwrap() != 2); // test that it changed
    
            // try the same assert_eqs
            assert_eq!(new_hashmap.get(&20).unwrap(), 5);
            assert_eq!(new_hashmap.get(&12).unwrap(), 5);
            assert_eq!(new_hashmap.get(&1).unwrap(), 1);
            assert_eq!(new_hashmap.get(&0).unwrap(), 0);
            assert!(new_hashmap.get(&3).unwrap() != 2); // test that it changed
        }
    }


