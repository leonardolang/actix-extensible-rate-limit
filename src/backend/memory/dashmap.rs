use crate::backend::memory::types::*;

use dashmap::DashMap;

pub type MemoryMap = DashMap<String, Value>;
