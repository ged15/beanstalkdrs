use std::collections::HashMap;

struct Job {
    id: u8,
    priority: u8,
    delay: u8,
    ttr: u8,
    data: Vec<u8>,
}

impl Job {
    fn new(id: u8, data: Vec<u8>) -> Job {
        Job {
            id: id,
            priority: 0,
            delay: 0,
            ttr: 1,
            data: data,
        }
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

    pub fn put(&mut self, pri: u8, delay: u8, ttr: u8, data: Vec<u8>) -> u8 {
        self.auto_increment_index += 1;

        debug!("Putting job ID {} with data {:?}", self.auto_increment_index, data);

        self.ready_jobs.insert(self.auto_increment_index, Job::new(self.auto_increment_index, data));

        self.auto_increment_index
    }

    pub fn reserve(&mut self) -> (u8, Vec<u8>) {
        // todo: can we use take(1) here?
        let key = self.ready_jobs.iter()
            .find(|&(_, &_)| true)
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
            Some(job) => {
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
            None => None,
        }
    }
}

pub struct StatsJobResponse {
    pub id: u8,
    pub tube: String,
    pub state: String,
    pub pri: u8,
    pub age: u8,
    pub delay: u8,
    pub ttr: u8,
    pub time: u8,
    pub file: u8,
    pub reserves: u8,
    pub timeouts: u8,
    pub releases: u8,
    pub buries: u8,
    pub kicks: u8,
}

enum JobState {
    Ready,
    Delayed,
    Reserved,
    Burried
}
