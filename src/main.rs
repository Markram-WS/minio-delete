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
    let path_prefix: Vec<&str> = vec!["sales_external.prod/"];

    let server = env::var("MINIO_SERVER")?;
    let user = env::var("MINIO_USER")?;
    let password = env::var("MINIO_PASS")?;

    let static_provider = StaticProvider::new(&user, &password, None);
    let base_url: BaseUrl = server.parse().unwrap();

    // minio prefix pool
    let (sub_dir_tx, sub_dir_rx) = mpsc::channel::<String>(20);
    let sub_dir_rx = Arc::new(Mutex::new(sub_dir_rx));
    // delete pull
    let (tx, rx) = mpsc::channel::<String>(1000);
    let rx = Arc::new(Mutex::new(rx));

    let minio = Client::new(base_url.clone(), Some(Box::new(static_provider.clone())), None,None)?;
    let shared_minio : Arc<Client> = Arc::new(minio);
    let shared_minio_spawn = Arc::clone(&shared_minio);
    let list_prefix = tokio::spawn(async move {
        for prefix in path_prefix {
            let minio_clone = Arc::clone(&shared_minio_spawn); 
            let minio_client = (*minio_clone).clone();
            let  stream = ListObjects::new(minio_client,bucket_name.to_string())
                .prefix(Some(prefix.to_string()) )
                .max_keys(Some(10000 as u16))
                .to_stream().await; 

                let mut stream = Box::pin(stream); // ถ้าต้อง pin

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(resp) => {
                            for item in resp.contents {
                                
                                //println!("->{:?}", &item.name);
                                let minio_client = (*minio_clone).clone();
                                let  stream_sub_dir = ListObjects::new(minio_client,bucket_name.to_string())
                                    .prefix(Some(item.name) )
                                    .max_keys(Some(10000 as u16))
                                    .to_stream().await; 
                                let mut stream_sub_dir = Box::pin(stream_sub_dir);
                                while let Some(result) = stream_sub_dir.next().await {
                                    match result {
                                        Ok(resp) => {
                                            for item in resp.contents {
                                                // ส่งชื่อไฟล์เข้า queue
                                                println!("\r[queue] <- {}",&item.name);
                                                println!("");
                                                if sub_dir_tx.send(item.name.clone()).await.is_err() {
                                                    return; // consumer ตาย → หยุด
                                                }
                                            }
                                        }
                                        Err(e) => eprintln!("list error: {:?}", e),
                                    }
                                }
                            
                            }   
                        }
                        Err(e) => eprintln!("Error listing objects: {:?}", e),
                        
                    }
                }
        }
    });

    // --------------
    // Task A: LIST (Producer)
    // --------------
    let workers_list_obj: i32 = 5; // get พร้อมกัน x ตัว
    let mut workers_list_obj_handles = Vec::new();
    for _ in 0..workers_list_obj {
        let minio_clone = Arc::clone(&shared_minio); 
        let tx = tx.clone();
        let w_sub_dir_rx = Arc::clone(&sub_dir_rx);
        let list_obj_handle = tokio::spawn(async move {
            loop {
                // ---- lock -> recv -> unlock ----
                let sub_dir: Option<String> = {
                    let mut guard = w_sub_dir_rx.lock().await;
                    guard.recv().await
                };
    
                let Some(sub_dir) = sub_dir else { break };
                let minio_client = (*minio_clone).clone(); 
                let stream = ListObjects::new(minio_client, (&bucket_name).to_string())
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
            }
            // drop tx เพื่อปิด channel → workers จะรู้ว่าหมดงานแล้ว
            
        });
        workers_list_obj_handles.push(list_obj_handle);
    }
    
    // --------------
    // Task B: DELETE worker pool (Consumers)
    // --------------
    let workers: i32 = 10; // ลบพร้อมกัน x ตัว
    let mut worker_handles = Vec::new();
    let global_counter = Arc::new(AtomicU64::new(0));
    for w in 0..workers {
        let minio_clone = Arc::clone(&shared_minio); 
        let rx = Arc::clone(&rx);
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
                let client: &minio::s3::Client = &*minio_clone; 
                let result = client.delete_object(bucket_name.clone(), obj).send().await;
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
    list_prefix.await?;
    for h in workers_list_obj_handles {
        let _ = h.await;
    }
    for h in worker_handles {
        let _ = h.await;
    }



    Ok(())
}
    
