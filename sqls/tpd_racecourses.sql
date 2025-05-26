CREATE SCHEMA IF NOT EXISTS tpd_hourse_race;
-- 1. Course table
CREATE TABLE tpd_hourse_race.courses (
    course_id SERIAL PRIMARY KEY,
    course_name TEXT UNIQUE NOT NULL
);

-- 2. Horse table
CREATE TABLE tpd_hourse_race.horses (
    horse_id SERIAL PRIMARY KEY,
    horse_name TEXT UNIQUE NOT NULL
);

-- 3. Race table
CREATE TABLE tpd_hourse_race.races (
    race_id TEXT PRIMARY KEY,
    post_time TIMESTAMP NOT NULL,
    course_id INTEGER NOT NULL,
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);

-- 4. Runners table
CREATE TABLE tpd_hourse_race.runners (
    runner_id SERIAL PRIMARY KEY,
    race_id TEXT NOT NULL,
    horse_id INTEGER NOT NULL,
    cloth_number INTEGER NOT NULL,
    starting_price TEXT,
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (horse_id) REFERENCES horses(horse_id),
    UNIQUE (race_id, cloth_number) -- Ensures uniqueness of cloth number per race
);

-- Indexes for performance
CREATE INDEX idx_runners_race_id ON tpd_hourse_race.runners(race_id);
CREATE INDEX idx_runners_horse_id ON tpd_hourse_race.runners(horse_id);
CREATE INDEX idx_races_post_time ON tpd_hourse_race.races(post_time);

-- ALTER tables queries to update constraints 
ALTER TABLE tpd_hourse_race.races DROP CONSTRAINT races_course_id_fkey;
ALTER TABLE tpd_hourse_race.races
  ADD CONSTRAINT races_course_id_fkey
  FOREIGN KEY (course_id) REFERENCES tpd_hourse_race.courses(course_id);

ALTER TABLE tpd_hourse_race.runners DROP CONSTRAINT races_course_id_fkey;
ALTER TABLE tpd_hourse_race.races
  ADD CONSTRAINT races_course_id_fkey
  FOREIGN KEY (course_id) REFERENCES tpd_hourse_race.courses(course_id);


ALTER TABLE tpd_hourse_race.runners DROP CONSTRAINT runners_race_id_fkey;
ALTER TABLE tpd_hourse_race.runners DROP CONSTRAINT runners_horse_id_fkey;

ALTER TABLE tpd_hourse_race.runners
  ADD CONSTRAINT runners_race_id_fkey
  FOREIGN KEY (race_id) REFERENCES tpd_hourse_race.races(race_id);

ALTER TABLE tpd_hourse_race.runners
  ADD CONSTRAINT runners_horse_id_fkey
  FOREIGN KEY (horse_id) REFERENCES tpd_hourse_race.horses(horse_id);