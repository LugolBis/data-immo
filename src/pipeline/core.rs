use crate::pipeline::task::{task1, task2, task3};

pub async fn main() {
    match task1().await {
        Ok(message) => println!("{}\n\nSuccessfully finished the Task1 !\n\n", message),
        Err(message) => {
            eprintln!("{}\n\nFailed to run the Task1.\n\n", message);
        }
    }

    match task2() {
        Ok(message) => println!("{}\n\nSuccessfully finished the Task2 !\n\n", message),
        Err(message) => {
            eprintln!("{}\n\nFailed to run the Task2.\n\n", message);
        }
    }
    
    match task3() {
        Ok(message) => println!("{}\n\nSuccessfully finished the Task3 !\n\n", message),
        Err(message) => {
            eprintln!("{}\n\nFailed to run the Task3.\n\n", message);
            return;
        }
    }
    
}
