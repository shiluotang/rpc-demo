namespace java org.sqg.thrift.generated

struct Student{
    1:string name,
    2:i32 age; 
}

service Greetings {
    string hello(1:Student s);
}