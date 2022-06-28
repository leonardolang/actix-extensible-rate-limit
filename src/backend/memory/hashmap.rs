use crate::backend::memory::types::*;

use core::borrow::Borrow;
use core::hash::Hash;

use std::cell::RefCell;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Mutex, MutexGuard};

use owning_ref::OwningHandle;

type HashMapCell<K,V> = RefCell<HashMap<K,V>>;

type EntryHandle<'a,K,V> = Box<Option<Entry<'a, K, V>>>;

type OwningEntryHandle<'a, K, V> = OwningHandle<MutexGuard<'a, HashMapCell<K,V>>, EntryHandle<'a, K, V>>;

// LockedEntry allows read/write access to HashMap entry API behind a global Mutex
pub struct LockedEntry<'a, K: 'a, V: 'a>(OwningEntryHandle<'a, K, V>);

impl<'a, K, V> LockedEntry<'a, K, V> {
    pub fn and_modify<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        // consumes the entry and leaves `None` inside the box, unwrap is safe since we will always...
        let entry = std::mem::replace(&mut *self.0, None).unwrap();
        // ... replace None with the resulting entry from `and_modify`
        *(&mut *self.0) = Some(entry.and_modify(f));
        self
    }

    pub fn or_insert_with<F>(mut self, default: F) -> &'a mut V
    where
        F: FnOnce() -> V,
    {
        // consumes entry, leaving `None` inside the box and returning a mut reference to the value
        let entry = std::mem::replace(&mut *self.0, None).unwrap();
        entry.or_insert_with::<F>(default)
    }
}

// LockedHashMap allows read/write access to a HashMap behind a global Mutex
pub struct LockedHashMap<K,V>(Mutex<HashMapCell<K,V>>);

impl<K,V> LockedHashMap<K,V>
where
    K: Eq + Hash
{
    pub fn new() -> Self {
        Self(Mutex::new(RefCell::new(HashMap::new())))
    }

    pub fn retain<F>(&self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool
    {
        let lock = self.0.lock().unwrap();
        let res = (*lock).borrow_mut().retain(f);
        res
    }

    pub fn entry(&self, k: K) -> LockedEntry<K, V>
    {
        let lock = self.0.lock().unwrap();
        // unsafe NOTE: retrieving a mutable reference from the HashMap inside the RefCell should be safe as long as
        // unsafe NOTE: exclusive access is guaranteed by holding the lock. this guarantee is encoded by keeping the
        // unsafe NOTE: MutexGuard and the Entry inside an OwningHandle which ties both lifetimes together
        LockedEntry(OwningHandle::new_with_fn(lock, |lock: *const RefCell<HashMap<K,V>>| {
            Box::new(Some((unsafe { &mut *((*lock).as_ptr()) }).entry(k)))
        }))
    }

    pub fn remove<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let lock = self.0.lock().unwrap();
        let res = (*lock).borrow_mut().remove(k);
        res
    }

    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let lock = self.0.lock().unwrap();
        let res = (*lock).borrow().contains_key(k);
        res
    }
}

pub type MemoryMap = LockedHashMap<String, Value>;
