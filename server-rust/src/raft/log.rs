use serde::{Deserialize, Serialize};

/// 日志条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new(term: u64, index: u64, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
        }
    }

    /// 创建 NoOp 条目（Leader 上任时的第一个条目）
    #[allow(dead_code)]
    pub fn no_op(term: u64, index: u64) -> Self {
        Self {
            term,
            index,
            command: vec![],
        }
    }
}

/// 内存日志存储
#[derive(Debug, Clone)]
pub struct LogStore {
    pub entries: Vec<LogEntry>,
}

impl LogStore {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 获取最后一条日志的索引
    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    /// 获取最后一条日志的任期
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// 获取指定索引的日志
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 || index > self.entries.len() as u64 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// 追加日志条目
    pub fn append(&mut self, entries: Vec<LogEntry>) {
        self.entries.extend(entries);
    }

    /// 追加单条日志
    pub fn append_one(&mut self, entry: LogEntry) -> u64 {
        let index = entry.index;
        self.entries.push(entry);
        index
    }

    /// 从指定索引截断日志
    pub fn truncate(&mut self, from_index: u64) {
        if from_index == 0 {
            self.entries.clear();
        } else {
            self.entries.truncate((from_index - 1) as usize);
        }
    }

    /// 获取从指定索引开始的所有日志
    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        if start_index == 0 || start_index > self.entries.len() as u64 {
            return vec![];
        }
        self.entries[(start_index - 1) as usize..].to_vec()
    }

    /// 检查指定位置的日志是否匹配
    pub fn match_entry(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true; // 空日志匹配
        }
        match self.get(prev_log_index) {
            Some(entry) => entry.term == prev_log_term,
            None => false,
        }
    }
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_log_store() {
        let log = LogStore::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    fn test_append_single_entry() {
        let mut log = LogStore::new();

        let entry = LogEntry::new(1, 1, vec![1, 2, 3]);
        log.append(vec![entry]);

        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.entries.len(), 1);
    }

    #[test]
    fn test_append_multiple_entries() {
        let mut log = LogStore::new();

        let entries = vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(1, 2, vec![2]),
            LogEntry::new(2, 3, vec![3]),
        ];
        log.append(entries);

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn test_get_entry() {
        let mut log = LogStore::new();

        log.append(vec![
            LogEntry::new(1, 1, vec![10]),
            LogEntry::new(2, 2, vec![20]),
        ]);

        // Get valid entries
        let entry1 = log.get(1);
        assert!(entry1.is_some());
        assert_eq!(entry1.unwrap().term, 1);
        assert_eq!(entry1.unwrap().command, vec![10]);

        let entry2 = log.get(2);
        assert_eq!(entry2.unwrap().term, 2);

        // Get invalid index
        assert!(log.get(0).is_none());
        assert!(log.get(3).is_none());
    }

    #[test]
    fn test_truncate() {
        let mut log = LogStore::new();

        log.append(vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(1, 2, vec![2]),
            LogEntry::new(2, 3, vec![3]),
        ]);

        // Truncate from index 2 (保留 index 1)
        log.truncate(2);

        assert_eq!(log.last_index(), 1);
        assert_eq!(log.entries.len(), 1);

        // Truncate from index 1 (清空)
        log.truncate(1);
        assert_eq!(log.last_index(), 0);
    }

    #[test]
    fn test_entries_from() {
        let mut log = LogStore::new();

        log.append(vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(1, 2, vec![2]),
            LogEntry::new(2, 3, vec![3]),
        ]);

        // Get entries from index 2
        let entries = log.entries_from(2);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[1].index, 3);

        // Get entries from index 1
        let entries = log.entries_from(1);
        assert_eq!(entries.len(), 3);

        // Invalid start index
        let entries = log.entries_from(4);
        assert_eq!(entries.len(), 0);

        let entries = log.entries_from(0);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_match_entry() {
        let mut log = LogStore::new();

        log.append(vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(1, 2, vec![2]),
        ]);

        // Match at index 1 with term 1
        assert!(log.match_entry(1, 1));

        // Match at index 2 with term 1
        assert!(log.match_entry(2, 1));

        // Mismatch at index 1 with term 2
        assert!(!log.match_entry(1, 2));

        // Match at index 0 (空日志匹配)
        assert!(log.match_entry(0, 0));

        // Mismatch at non-existing index
        assert!(!log.match_entry(3, 1));
    }

    #[test]
    fn test_append_one() {
        let mut log = LogStore::new();

        let index = log.append_one(LogEntry::new(1, 1, vec![1]));
        assert_eq!(index, 1);

        let index = log.append_one(LogEntry::new(1, 2, vec![2]));
        assert_eq!(index, 2);
    }

    #[test]
    fn test_no_op_entry() {
        let entry = LogEntry::no_op(5, 10);
        assert_eq!(entry.term, 5);
        assert_eq!(entry.index, 10);
        assert_eq!(entry.command, Vec::<u8>::new());
    }
}
