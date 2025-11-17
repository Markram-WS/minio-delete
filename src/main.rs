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
    let concurrency = 10;
    for sub_dir in sub_dir_prefix {
        println!("");
        println!("-> {}",&sub_dir);
        // สร้าง stream ของ object จาก sub_dir
        let stream_sub_dir = ListObjects::new(minio.clone(), bucket_name.to_string())
            .prefix(Some(sub_dir.to_string()))
            .recursive(true)
            .max_keys(Some(10000))
            .to_stream().await;
    
        // iterate ผ่าน stream ทีละ response
        let mut stream_sub_dir = Box::pin(stream_sub_dir);
        let mut nitems = 1;
        while let Some(result) = stream_sub_dir.next().await {
            match result {
                Ok(resp) => {
                    for item in resp.contents {
                        let obj = ObjectToDelete::from(&item.name);
                        let delete_result= minio.delete_object(bucket_name.clone(),obj).send().await;
                        match delete_result {
                            Ok(_) => print!("\rDel {} {}", nitems, item.name),
                            Err(e) => eprintln!("Error deleting object: {:?}", e),
                        }
                        nitems += 1;
                    }
                    print!("")
                }
                Err(e) => eprintln!("Error listing objects: {:?}", e),
            }
        }
    }
    
    // while let Some(item) = stream.next().await {
    //     match item {
    //         Ok(obj) => {
    //             println!("Object Key: {}", obj.key);
    //         }
    //         Err(e) => eprintln!("Error: {:?}", e),
    //     }
    // }

    // ---- สำคัญที่สุด ----
    // กำหนดว่าให้ลบพร้อมกันสูงสุดกี่อัน
    // stream.map(...) = แปลง object → future ของการ delete
    // buffer_unordered(concurrency) = run พร้อมกันได้สูงสุด 10 อัน
    

    Ok(())
}
    
