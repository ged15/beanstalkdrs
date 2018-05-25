use std::collections::HashMap;

pub struct Job {
    id: u8,
    data: Vec<u8>,
}

impl Job {
    fn new(id: u8, data: Vec<u8>) -> Job {
        Job { id: id, data: data }
    }
}

struct TubeName {
    value: String
}

impl TubeName {
    fn new(value: String) -> TubeName {
        TubeName { value }
    }
}

pub struct JobQueue {
    ready_jobs: HashMap<u8, Job>,
    reserved_jobs: HashMap<u8, Job>,
    auto_increment_index: u8,
}

impl JobQueue {
    pub fn new() -> JobQueue {
        JobQueue {
            ready_jobs: HashMap::new(),
            reserved_jobs: HashMap::new(),
            auto_increment_index: 0,
        }
    }

    pub fn use_tube(&mut self, tube_name: TubeName) {}

    #[allow(unused_variables)]
    pub fn put(&mut self, pri: u8, delay: u8, ttr: u8, data: Vec<u8>) -> u8 {
        self.auto_increment_index += 1;

        debug!("Putting job ID {} with data {:?}", self.auto_increment_index, data);

        self.ready_jobs.insert(self.auto_increment_index, Job::new(self.auto_increment_index, data));

        self.auto_increment_index
    }

    pub fn reserve(&mut self) -> (u8, Vec<u8>) {
        let key = self.ready_jobs.iter()
            .next()
            .map(|(key, _)| key.clone());

        match key {
            Some(id) => {
                let job = self.ready_jobs.remove(&id).unwrap();

                let ret = (id, job.data.clone());

                self.reserved_jobs.insert(id, Job::new(job.id, job.data.clone()));

                ret
            },
            None => panic!("No more jobs!"),
        }
    }

    pub fn delete(&mut self, id: &u8) -> Option<Job> {
        debug!("Deleting job {}", id);

        match self.ready_jobs.remove(id) {
            Some(job) => Some(job),
            None => self.reserved_jobs.remove(id),
        }
    }

    pub fn release(&mut self, id: &u8) -> Option<Job> {
        debug!("Releasing job {}", id);

        let key = self.reserved_jobs.iter()
            .find(|&(job_id, &_)| job_id == id)
            .map(|(key, _)| key.clone());

        match key {
            Some(id) => {
                let job = self.reserved_jobs.remove(&id).unwrap();
                self.ready_jobs.insert(id, Job::new(job.id, job.data.clone()));
                Some(job)
            },
            None => None,
        }
    }

    pub fn peek_ready(&self) -> Option<(u8, Vec<u8>)> {
        match self.ready_jobs.iter().next() {
            Some((id, job)) => Some((id.clone(), job.data.clone())),
            None => None,
        }
    }

    pub fn stats_job(&self, id: &u8) -> Option<StatsJobResponse> {
        match self.ready_jobs.get(id) {
            Some(_) => {
                Some(StatsJobResponse {
                    id: *id,
                    tube: "default".to_string(),
                    state: "ready".to_string(),
                    pri: 0,
                    age: 0,
                    delay: 0,
                    ttr: 0,
                    time: 0,
                    file: 0,
                    reserves: 0,
                    timeouts: 0,
                    releases: 0,
                    buries: 0,
                    kicks: 0,
                })
            },
            None => {
                match self.reserved_jobs.get(id) {
                    Some(_) => {
                        Some(StatsJobResponse {
                            id: *id,
                            tube: "default".to_string(),
                            state: "reserved".to_string(),
                            pri: 0,
                            age: 0,
                            delay: 0,
                            ttr: 0,
                            time: 0,
                            file: 0,
                            reserves: 0,
                            timeouts: 0,
                            releases: 0,
                            buries: 0,
                            kicks: 0,
                        })
                    },
                    None => None,
                }
            },
        }
    }

    pub fn stats_tube(&self) -> Option<StatsTubeResponse> {
        Some(StatsTubeResponse {
            current_jobs_ready: self.ready_jobs.len(),
            current_jobs_reserved: self.reserved_jobs.len(),
            total_jobs: self.ready_jobs.len() + self.reserved_jobs.len(),
        })
    }
}

pub struct StatsJobResponse {
    id: u8,
    tube: String,
    state: String,
    pri: u8,
    age: u8,
    delay: u8,
    ttr: u8,
    time: u8,
    file: u8,
    reserves: u8,
    timeouts: u8,
    releases: u8,
    buries: u8,
    kicks: u8,
}

impl StatsJobResponse {
    pub fn to_string(&self) -> String {
        let yaml = format!(
            "---\n\
id: {}\n\
tube: {}\n\
state: {}\n\
pri: {}\n\
age: {}\n\
delay: {}\n\
ttr: {}\n\
time: {}\n\
file: {}\n\
reserves: {}\n\
timeouts: {}\n\
releases: {}\n\
buries: {}\n\
kicks: {}\n",
            self.id,
            self.tube,
            self.state,
            self.pri,
            self.age,
            self.delay,
            self.ttr,
            self.time,
            self.file,
            self.reserves,
            self.timeouts,
            self.releases,
            self.buries,
            self.kicks,
        );

        format!("OK {}\r\n{}\r\n", yaml.len(), yaml)
    }
}

pub struct StatsTubeResponse {
    current_jobs_ready: usize,
    current_jobs_reserved: usize,
    total_jobs: usize,
}

impl StatsTubeResponse {
    pub fn to_string(&self) -> String {
        let stats = format!(
            "---
name: default
current-jobs-urgent: 0
current-jobs-ready: {}
current-jobs-reserved: {}
current-jobs-delayed: 0
current-jobs-buried: 0
total-jobs: {}
current-using: 0
current-waiting: 0
current-watching: 0
pause: 0
cmd-delete: 0
cmd-pause-tube: 0
pause-time-left: 0
",
        self.current_jobs_ready,
        self.current_jobs_reserved,
        self.total_jobs
        );
        format!(
            "OK {}\r\n{}\r\n",
            stats.len(),
            stats
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_job_checks_ready_and_reserved_jobs() {
        let mut sut = JobQueue::new();

        let id1 = sut.put(1, 1, 1, "job1".to_string().into_bytes());
        let id2 = sut.put(1, 1, 1, "job2".to_string().into_bytes());

        let (reserved_job_id, _) = sut.reserve();

        assert!(sut.stats_job(&id1).is_some());
        assert!(sut.stats_job(&id2).is_some());
        assert!(sut.stats_job(&reserved_job_id).is_some());
    }

    #[test]
    fn delete_checks_ready_and_reserved_jobs() {
        let mut sut = JobQueue::new();

        let id1 = sut.put(1, 1, 1, "job1".to_string().into_bytes());
        let id2 = sut.put(1, 1, 1, "job2".to_string().into_bytes());

        let (reserved_job_id, _) = sut.reserve();

        if id1 != reserved_job_id {
            assert!(sut.delete(&id1).is_some());
        }

        if id2 != reserved_job_id {
            assert!(sut.delete(&id2).is_some());
        }

        assert!(sut.delete(&reserved_job_id).is_some());
    }

    #[test]
    fn putting_job_into_specific_queue() {
        let mut sut = JobQueue::new();

        sut.use_tube(TubeName::new("some_tube".to_string()));
        let id1 = sut.put(1, 1, 1, "job1".to_string().into_bytes());

        sut.use_tube(TubeName::new("another_tube".to_string()));
        let (reserved_job_id, _) = sut.reserve();
    }
}
