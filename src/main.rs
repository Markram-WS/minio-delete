use minio::s3::Client;
use minio::s3::builders::ListObjects;
use minio::s3::builders::ObjectToDelete;
use minio::s3::types::S3Api;
use minio::s3::creds::StaticProvider;
use minio::s3::types::ToStream;
use std::env;
use dotenv::dotenv;
use tokio_stream::StreamExt;
use minio::s3::http::BaseUrl;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let bucket_name = "reportingdb";
    let path_prefix = vec!["sales_external.prod/"];

    let server = env::var("MINIO_SERVER")?;
    let user = env::var("MINIO_USER")?;
    let password = env::var("MINIO_PASS")?;

    let static_provider = StaticProvider::new(&user, &password, None);
    let base_url: BaseUrl = server.parse().unwrap();
    let minio = Client::new(base_url, Some(Box::new(static_provider)), None,None)?;


    let mut dir_prefix : Vec<String> = Vec::new();
    let mut sub_dir_prefix : Vec<String> = Vec::new();

    for prefix in path_prefix {
        let  stream = ListObjects::new(minio.clone(),bucket_name.to_string())
            .prefix(Some(prefix.to_string()) )
            .max_keys(Some(10000 as u16))
            .to_stream().await; 

            let mut stream = Box::pin(stream); // ถ้าต้อง pin

            while let Some(result) = stream.next().await {
                match result {
                    Ok(resp) => {
                        for item in resp.contents {
                            
                            println!("->{:?}", &item.name);
                            let  stream_sub_dir = ListObjects::new(minio.clone(),bucket_name.to_string())
                                .prefix(Some(item.name) )
                                .max_keys(Some(10000 as u16))
                                .to_stream().await; 
                            let mut stream_sub_dir = Box::pin(stream_sub_dir);
                            while let Some(result) = stream_sub_dir.next().await {
                                match result {
                                        Ok(resp) => {
                                            for item in resp.contents {
                                                
                                                println!("      |_{:?}", &item.name);
                                                sub_dir_prefix.push(item.name);
                                            }
                                        }
                                        Err(e) => eprintln!("Error listing objects: {:?}", e),
                                    }
                            }
                        
                        }   
                    }
                    Err(e) => eprintln!("Error listing objects: {:?}", e),
                    
                }
            }
    }
    
    //let first5: Vec<String> = sub_dir_prefix.iter().take(10).cloned().collect();
    for sub_dir in sub_dir_prefix {
        println!("");
        println!("-> {}",&sub_dir);

        let (tx, rx) = mpsc::channel::<String>(1000);
        let rx = Arc::new(Mutex::new(rx));
        // --------------
        // Task A: LIST (Producer)
        // --------------
        let minio_list = minio.clone();
        let bucket = bucket_name.clone();
        let sub_dir = sub_dir.to_string();

        let list_task = tokio::spawn(async move {
            let stream = ListObjects::new(minio_list.clone(), bucket.to_string())
                .prefix(Some(sub_dir))
                .recursive(true)
                .max_keys(Some(10000))
                .to_stream()
                .await;
        
            let mut stream = Box::pin(stream);
        
            while let Some(result) = stream.next().await {
                match result {
                    Ok(resp) => {
                        for item in resp.contents {
                            // ส่งชื่อไฟล์เข้า queue
                            if tx.send(item.name.clone()).await.is_err() {
                                return; // consumer ตาย → หยุด
                            }
                        }
                    }
                    Err(e) => eprintln!("list error: {:?}", e),
                }
            }
            // drop tx เพื่อปิด channel → workers จะรู้ว่าหมดงานแล้ว
        });

        // --------------
        // Task B: DELETE worker pool (Consumers)
        // --------------
        let workers = 8; // ลบพร้อมกัน x ตัว
        let mut worker_handles = Vec::new();
        let global_counter = Arc::new(AtomicU64::new(0));
        
        for w in 0..workers {
            let rx = Arc::clone(&rx);
            let minio_del = minio.clone();
            let counter = global_counter.clone();
            let handle = tokio::spawn(async move {
                loop {
                    // ---- lock -> recv -> unlock ----
                    let key_opt: Option<String> = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };
        
                    // channel closed -> exit worker
                    let Some(key) = key_opt else { break };
        
                    let obj: ObjectToDelete = ObjectToDelete::from(&key);
                    let result =minio_del.delete_object(bucket.to_string(), obj).send().await;
                    match result {
                        Ok(_) => {
                            let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
                            print!("\r{}-{} `del` {}",&n,&w,&key);
                            
                        }
                        Err(e) => eprintln!("[{}] delete {} error: {:?}", &w,&key, e),
                    }
                }
            });
            worker_handles.push(handle);
        }
        // --------------
        // Wait
        // --------------
        list_task.await?;
        for h in worker_handles {
            let _ = h.await;
        }
    }


    Ok(())
}
    
