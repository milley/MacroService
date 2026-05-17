use rand::Rng;
use std::time::{Duration, Instant};

/// 选举超时定时器
#[derive(Debug)]
pub struct ElectionTimer {
    /// 超时时间
    pub timeout: Duration,
    /// 最后一次收到心跳的时间
    last_heartbeat: Instant,
    /// 最小超时（毫秒）
    min_timeout_ms: u64,
    /// 最大超时（毫秒）
    max_timeout_ms: u64,
}

impl ElectionTimer {
    pub fn new() -> Self {
        Self {
            timeout: Self::random_timeout(150, 300),
            last_heartbeat: Instant::now(),
            min_timeout_ms: 150,
            max_timeout_ms: 300,
        }
    }

    /// 创建指定范围的定时器
    #[allow(dead_code)]
    pub fn with_range(min_ms: u64, max_ms: u64) -> Self {
        Self {
            timeout: Self::random_timeout(min_ms, max_ms),
            last_heartbeat: Instant::now(),
            min_timeout_ms: min_ms,
            max_timeout_ms: max_ms,
        }
    }

    /// 生成随机超时
    fn random_timeout(min_ms: u64, max_ms: u64) -> Duration {
        let mut rng = rand::rng();
        let ms = rng.random_range(min_ms..=max_ms);
        Duration::from_millis(ms)
    }

    /// 重置定时器（收到心跳时调用）
    pub fn reset(&mut self) {
        self.last_heartbeat = Instant::now();
        self.timeout = Self::random_timeout(self.min_timeout_ms, self.max_timeout_ms);
    }

    /// 检查是否超时（应该发起选举）
    pub fn is_expired(&self) -> bool {
        self.last_heartbeat.elapsed() >= self.timeout
    }

    /// 获取剩余时间
    #[allow(dead_code)]
    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.last_heartbeat.elapsed())
    }
}

impl Default for ElectionTimer {
    fn default() -> Self {
        Self::new()
    }
}

/// 心跳定时器（Leader 使用）
#[derive(Debug)]
pub struct HeartbeatTimer {
    pub interval: Duration,
    last_beat: Instant,
}

impl HeartbeatTimer {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval: Duration::from_millis(interval_ms),
            last_beat: Instant::now(),
        }
    }

    pub fn reset(&mut self) {
        self.last_beat = Instant::now();
    }

    pub fn should_beat(&self) -> bool {
        self.last_beat.elapsed() >= self.interval
    }

    #[allow(dead_code)]
    pub fn remaining(&self) -> Duration {
        self.interval.saturating_sub(self.last_beat.elapsed())
    }
}

impl Default for HeartbeatTimer {
    fn default() -> Self {
        Self::new(50) // 默认 50ms 心跳间隔
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_election_timer_new() {
        let timer = ElectionTimer::new();

        // 新创建的定时器不应该超时
        assert!(!timer.is_expired());

        // 超时应该在 150-300ms 范围内
        assert!(timer.timeout.as_millis() >= 150);
        assert!(timer.timeout.as_millis() <= 300);
    }

    #[test]
    fn test_election_timer_with_range() {
        let timer = ElectionTimer::with_range(100, 200);

        assert!(timer.timeout.as_millis() >= 100);
        assert!(timer.timeout.as_millis() <= 200);
    }

    #[test]
    fn test_election_timer_reset() {
        let mut timer = ElectionTimer::new();

        // 等待一小段时间
        sleep(Duration::from_millis(10));

        // 重置
        timer.reset();

        // 重置后不应该超时
        assert!(!timer.is_expired());
    }

    #[test]
    fn test_election_timer_expired() {
        let timer = ElectionTimer::with_range(10, 20);

        // 等待超时
        sleep(Duration::from_millis(30));

        assert!(timer.is_expired());
    }

    #[test]
    fn test_election_timer_remaining() {
        let timer = ElectionTimer::with_range(100, 100);

        let remaining = timer.remaining();
        assert!(remaining.as_millis() <= 100);
    }

    #[test]
    fn test_heartbeat_timer_new() {
        let timer = HeartbeatTimer::new(50);

        assert_eq!(timer.interval.as_millis(), 50);
        assert!(!timer.should_beat());
    }

    #[test]
    fn test_heartbeat_timer_should_beat() {
        let timer = HeartbeatTimer::new(10);

        // 等待超过间隔
        sleep(Duration::from_millis(20));

        assert!(timer.should_beat());
    }

    #[test]
    fn test_heartbeat_timer_reset() {
        let mut timer = HeartbeatTimer::new(10);

        // 等待超过间隔
        sleep(Duration::from_millis(20));
        assert!(timer.should_beat());

        // 重置
        timer.reset();
        assert!(!timer.should_beat());
    }
}
