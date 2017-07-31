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
        self.ready_jobs.insert(self.auto_increment_index, Job::new(self.auto_increment_index, data));

        self.auto_increment_index
    }

    pub fn reserve(&mut self) -> (u8, Vec<u8>) {
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
        println!("Deleting job {}", id);
        match self.ready_jobs.remove(id) {
            Some(job) => Some(job),
            None => self.reserved_jobs.remove(id),
        }
    }

    pub fn release(&mut self, id: &u8) -> Option<Job> {
        println!("Releasing job {}", id);

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
}
