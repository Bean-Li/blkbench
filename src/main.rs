#[macro_use]
extern crate lazy_static;
extern crate ansi_term;
extern crate getopts;
extern crate chrono;
extern crate serde_json;

use std::fmt;
use std::env;
use std::process::Command;
use getopts::Options;
use std::collections::HashMap;
use ansi_term::Colour;
use serde_json::{Value};

#[derive (Clone)]
pub struct FioConfig {
    thread: u32,
    direct: u32,
    norandommap:u32 ,
    randrepeat: u32 ,
    time_based: bool ,
    group_reporting:bool ,
    stonewall: bool,
    ioengine: String,
    rw: String,
    rwmixread: u32,
    bs: String,

    numjobs: u32,
    iodepth: u32,
    runtime: u32,
    size: String,
    filename: String,
}

impl FioConfig {
    fn new(filename: &str, size: &str, runtime: u32, rwmode: &str, rwmixread: u32, bs: &str, numjobs: u32, iodepth:u32) -> FioConfig{
        FioConfig{
            thread: 1,
            direct: 1,
            norandommap:1 ,
            randrepeat: 0 ,
            time_based: true ,
            group_reporting: true ,
            stonewall: true,
            ioengine: "libaio".into(),
            rw: rwmode.to_string(),
            rwmixread: rwmixread,
            bs: bs.to_string(),

            numjobs: numjobs,
            iodepth: iodepth,
            runtime: runtime,
            size: size.to_string(),
            filename: filename.to_string()
        }
    }


}

impl fmt::Display for FioConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        let mut content: String = format!("[global]\n"); 
        content = format!("{}thread={}\n",content, self.thread);
        content = format!("{}direct={}\n",content, self.direct);
        content = format!("{}timebased\n",content);
        content = format!("{}runtime={}\n",content, Colour::Yellow.paint(self.runtime.to_string()));
        content = format!("{}numjobs={}\n",content, Colour::Yellow.paint(self.numjobs.to_string()));
        content = format!("{}randrepeat={}\n",content, self.randrepeat);
        content = format!("{}norandommap={}\n",content, self.norandommap);
        content = format!("{}group_reporting\n",content);


        content = format!("{}\n[config]\n", content);
        content = format!("{}stonewall\n", content);
        content = format!("{}rw={}\n", content, Colour::Yellow.paint(&self.rw));
        content = format!("{}rwmixread={}\n", content, Colour::Yellow.paint(self.rwmixread.to_string()));
        content = format!("{}ioengine={}\n", content, self.ioengine);
        content = format!("{}iodepth={}\n", content, Colour::Yellow.paint(self.iodepth.to_string()));
        content = format!("{}bs={}\n", content, Colour::Yellow.paint(&self.bs));
        content = format!("{}size={}\n", content, Colour::Yellow.paint(&self.size));
        content = format!("{}filename={}\n", content, Colour::Yellow.paint(&self.filename));

        write!(f, "{}",content)
    }
}

lazy_static! {
    static ref MAP: HashMap<String, FioConfig> = {
        let mut map = HashMap::new();
        map.insert(String::from("random_r"),    FioConfig::new("/dev/sdzz", "10G", 20, "randread", 100, "1M", 1 , 8));
        map.insert(String::from("random_w"),    FioConfig::new("/dev/sdzz", "10G", 20, "randwrite",  0, "1M", 1 , 8));
        map.insert(String::from("random_7r3w"), FioConfig::new("/dev/sdzz", "10G", 20, "randrw",    70, "1M", 1 , 8));
        map.insert(String::from("seq_r"),       FioConfig::new("/dev/sdzz", "10G", 20, "read",     100, "1M", 1 , 8));
        map.insert(String::from("seq_w"),       FioConfig::new("/dev/sdzz", "10G", 20, "write",      0, "1M", 1 , 8));
        map.insert(String::from("seq_7r3w"),    FioConfig::new("/dev/sdzz", "10G", 20, "rw",        70, "1M", 1 , 8));
        map.insert(String::from("seq_9r1w"),    FioConfig::new("/dev/sdzz", "10G", 20, "rw",        90, "1M", 1 , 8));
        map
    };
}

fn print_usage(program :&str, opts :Options)
{
    let brief = format!("Usage: {} [options]",program);
    print!("{}", opts.usage(&brief));
}

pub fn find_fio_pattern(name: &String) -> Option<FioConfig>
{
    MAP.get(name).cloned()
}

pub fn print_fio_pattern(job_names: Vec<String>)
{
    for p_name in &job_names
    {
        match find_fio_pattern(p_name){
            None => println!("{} is Unknown fio pattern", p_name),
            Some(config) => println!("{}", config),
        }
    }
}

fn list_fio_pattern()
{
    for key in MAP.keys()
    {
        println!("{}", key);
    }
}


fn exec_fio_job(job: &str, filename: &str, size: &str, runtime: u32, bs: &str, numjobs:u32, iodepth: u32)
{
    let sample_config =  match find_fio_pattern(&job.to_string()) {
        None=>panic!("Invalidate FIO Pattern {}", job),
        Some(config)=> config ,
    };

    let fio_job = FioConfig::new(filename, size, runtime, &sample_config.rw, sample_config.rwmixread, bs, numjobs, iodepth);
    let fio_job_name: String = format!("{}_{}_{}", job, bs, iodepth);
    println!("==========={}==============", fio_job_name);
    println!("{}", fio_job);

    let begin = chrono::Utc::now();
    let output = Command::new("fio").arg(format!("--name={}", fio_job_name))
                                    .arg(format!("--filename={}",fio_job.filename))
                                    .arg(format!("--size={}",fio_job.size))
                                    .arg(format!("--runtime={}",fio_job.runtime))
                                    .arg(format!("--rw={}",fio_job.rw))
                                    .arg(format!("--rwmixread={}",fio_job.rwmixread))
                                    .arg(format!("--ioengine={}",fio_job.ioengine))
                                    .arg(format!("--bs={}",fio_job.bs))
                                    .arg(format!("--iodepth={}",fio_job.iodepth))
                                    .arg(format!("--numjobs={}",fio_job.numjobs))
                                    .arg(format!("--direct={}",fio_job.direct))
                                    .arg(format!("--thread={}",fio_job.thread))
                                    .arg(format!("--norandommap={}",fio_job.norandommap))
                                    .arg(format!("--randrepeat={}",fio_job.randrepeat))
                                    .arg(format!("--time_based"))
                                    .arg(format!("--group_reporting"))
                                    .arg(format!("--stonewall"))
                                    .arg(format!("--output-format=json"))
                                    .output()
                                    .expect("failed to exec fio")  ;
    let end = chrono::Utc::now();
    let result = String::from_utf8_lossy(&output.stdout) ;

    let v: Value = serde_json::from_str(&result).expect("invalid json result");

    println!("               {} ~ {}", begin.format("%Y-%m-%d %H:%M:%S UTC"),
                        end.format("%Y-%m-%d %H:%M:%S UTC"));
    println!("{}", "-".repeat(95));
    println!("| type|      iops| bandwidth| latency_avg| latency_90%| latency_95%| latency_99%|latency_99.9%|");
    println!("+{}+{}+{}+{}+{}+{}+{}+{}+", "-".repeat(5),
                                          "-".repeat(10),
                                          "-".repeat(10),
                                          "-".repeat(12),
                                          "-".repeat(12),
                                          "-".repeat(12),
                                          "-".repeat(12),
                                          "-".repeat(13));
    for item in vec!["read", "write"]
    {
        let  name ;
        if item == "read"
        {
            name = "read";
        }
        else
        {
            name = "write";
        }

        let iops = (v["jobs"][0][item]["iops"]).as_f64().unwrap();
        let bandwidth = (v["jobs"][0][item]["bw"]).as_f64().unwrap();
        let latency_avg = (v["jobs"][0][item]["lat_ns"]["mean"]).as_f64().unwrap();
        let latency_900 = (v["jobs"][0][item]["clat_ns"]["percentile"]["90.000000"]).as_f64().unwrap();
        let latency_950 = (v["jobs"][0][item]["clat_ns"]["percentile"]["95.000000"]).as_f64().unwrap();
        let latency_990 = (v["jobs"][0][item]["clat_ns"]["percentile"]["99.000000"]).as_f64().unwrap();
        let latency_999 = (v["jobs"][0][item]["clat_ns"]["percentile"]["99.900000"]).as_f64().unwrap();
        {
            print!("|{item:>5}|{iops:>10.2}|{bandwidth:>10.2}|{latency_avg:>12.3}|{latency_900:>12.3}|{latency_950:>12.3}|{latency_990:>12.3}|{latency_999:>13.3}|\n",
                   item=name, iops=iops,
                   bandwidth=bandwidth/1024.0,
                   latency_avg=latency_avg/1000000.0,
                   latency_900=latency_900/1000000.0,
                   latency_950=latency_950/1000000.0,
                   latency_990=latency_990/1000000.0,
                   latency_999=latency_999/1000000.0);
            if name == "read"
            {
                println!("+{}+{}+{}+{}+{}+{}+{}+{}+", "-".repeat(5),
                                                      "-".repeat(10),
                                                      "-".repeat(10),
                                                      "-".repeat(12),
                                                      "-".repeat(12),
                                                      "-".repeat(12),
                                                      "-".repeat(12),
                                                      "-".repeat(13));
            }
            else
            {
                println!("{}", "-".repeat(95));
            }

        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();

    opts.optflag("h", "help", "display this help and exit");
    opts.optflag("l","listjobs", "list fio pattern we support");
    opts.optflag("r","random", "run all fio jobs in random order");
    opts.optflag("p","printjobs","print fio pattern specified by [type:jobs]");
    opts.optopt("j", "jobs", "specify fio patterns we try to test [pattern1, pattern2]", "");
    opts.optopt("d", "destination", "filename parameter in fio, for iscsi it is a device like /dev/sdx, for nas it is directory", "");
    opts.optopt("s", "workset", "size parameter in fio", "");
    opts.optopt("t", "runtime","runtime of fio test", "");
    opts.optopt("q", "iodepth", "iodepth of fio test", "");
    opts.optopt("b", "bs",      "blksize of fio test, default is 4K,64K,128K,1M", "");
    opts.optopt("n", "numjobs", "numjobs of fio script, default is 8", "");
    opts.optopt("f", "format", "output format, support [raw|json|yaml]", "");

    let matches = match opts.parse(&args[1..]){
        Ok(m) => m,
        Err(f) => {
            eprintln!("{}", f);
            std::process::exit(1);
        }
    };

    let help = if matches.opt_present("h") {true} else {false};
    let listjobs = if matches.opt_present("l") {true} else {false} ;
    let _random = if matches.opt_present("r") {true} else {false} ;
    let printjobs = if matches.opt_present("p") {true} else {false} ;

    let mut all_jobs = false;

    let jobstr = match matches.opt_str("j"){
        Some(s) => s,
        None =>"all".to_string()
    };

    let iodepth_str = match matches.opt_str("q"){
        Some(s) => s,
        None=> "1,4,8,32".to_string()
    };

    let bs_str = match matches.opt_str("b"){
        Some(s) => s,
        None=> "4k,64k,128K,1M".to_string()
    };

    let iodepths : Vec<u32>= iodepth_str.split(",").map(|x| x.parse::<u32>().unwrap()).collect();

    let bs_s: Vec<String> = bs_str.split(',').map(|x| x.to_string()).collect();

    let mut jobs: Vec<String>  = [].to_vec();
    if jobstr == "all".to_string()
    {
        all_jobs = true;
        for k in MAP.keys()
        {
            jobs.push(k.to_string());
        }
    }
    else
    {
        jobs = jobstr.split(",").map(|x| x.to_string()).collect();
        jobs.sort();
    }


    if help
    {
        print_usage(&program, opts);
        return ;
    }

    if listjobs
    {
        list_fio_pattern();
        return ;
    }

    if printjobs
    {
        if !all_jobs
        {
            print_fio_pattern(jobs);
            return ;
        }
        else
        {
            eprintln!("you should specify jobs by  -j [job1,job2]");
            print_usage(&program, opts);
            return 
        }
    }

    let filename = match matches.opt_str("d"){
        Some(s) => s,
        None => panic!("You must specify filename by -d [filename] ")
    };

    let size = match matches.opt_str("s"){
        Some(s) => s,
        None => panic!("You must specify workset by -s [size]")
    };

    let time = match matches.opt_str("t"){
        Some(s) => s.parse::<u32>().unwrap() ,
        None => panic!("You must specify runtime by -t [runtime]")
    };

    let numjobs = match matches.opt_str("n"){
        Some(s) => s.parse::<u32>().unwrap(),
        None => "8".parse::<u32>().unwrap()
    };

    for job in &jobs
    {
        for bs in &bs_s
        {
            for iodepth in iodepths.iter()
            {
                exec_fio_job(job, &filename, &size, time, bs, numjobs, *iodepth )
            }
        }
    }
}
