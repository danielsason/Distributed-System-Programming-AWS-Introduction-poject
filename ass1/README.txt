Daniel Sason 304995111
Gabriel Lazard 305749335

Instructions how to run our project:
simply write the following in the commandLine:
java -jar assignment1-1.0.0-jar-with-dependencies.jar inputFileName outputFileName n
where thr arguments are as specified in the assignment instructions, or if you want to terminate the system:
java -jar assignment1-1.0.0-jar-with-dependencies.jar inputFileName outputFileName n terminate


Our project:

Our project uses only one pre-existing bucket called "project-jar-gabi-daniel" which should exist before the run of a "LocalApp",
as it contains the jar of the project which the EC2 nodes must download and run.
By using Tags, we make sure there will always be only 1 "Manager" instance running at all times(until the "Manager" receives a termination message ofcourse)
All the different components of the system use SQS to communicate with each other.
The Messages themselves are in JSON format meaning they can be easily transformed to simple Java classes
We used 3 classes that implement Runnable interface to help the "Manager" do its job (see Manager side).


localApp side:
the localApp resides on a local "user" machine.
the localApp will read the arguments given to it, initiate all the AWS services that need initializing(including turning on a manager instance, if one isnt running), create a 'NewTask' object and send it to the "Manager", 
waiting for a 'DoneTask' message from him in a loop that ends once he gets it or if there no active "Manager" instance, meaning another user shut the system down,
in which case an appropriate message will be presented to the user of the "LocalApp".

notice, when localApp sent a 'newTask' with a terminate flag, the newTask will be sent without the flag,
and only after got a 'doneTask' message, the localApp will send a NewTask message with contains only the termination flag, letting the manager know he should terminate the whole system.


The communication between the "LocalApp" and "Manager" works as follows:
-NewTask message sent from "localApp" to "Manager" through one const queue that's created once by the "LocalApp" which activates the "Manager" instance aka 'local_to_manager' queue.
-DoneTask message sent from Manager to "LocalApp" through a unique queue per localApp.
 the queueUrl (one per "LocalApp") is sent as a part of the NewTask message to the Manager that organizes the job requirments.


Manager side:
manager has 3 kinds of hashmap:
	we use the SQS messageId of the NewTask sent to the manager as unigue way to seperate the different NewTasks the Manager receives aka taskId
	-taskId_to_tasks: <String taskId, ArrayList<DonePDFTask>> : this hash stores all the donePDFtask (worker's job) related to taskId
	-taskId_size: <String taskId, Integer task's_pdfs> : this hash stores the amounts of pdfTask related to taskId;
	-taskId_localAppQueue: <String taskId, String queueUrl> : this hash map stores the manager_to_localApp queue to appropriate taskId.

//////first, the Manager initzializes a pool of 10 donePDFtaskHandler threads, then
Manager works in a loop until he gets a termination message:
	-receive a newTask messages from localApps, update taskId_localAppQueue hashtable as necessary (to find later the queueUrl) and initialize newTaskHandler thread.

Manager used a number of tools for his job (the three are threads, and there's a thread pool of 10 threads for each type):
-newTaskHandler - once manager accept a newTask message newTaskHandler handle it as follow:
	-download the input file from the localApp.
	-creates a list of newPDFtasks, and update the taskId_size hashmap. 
	-creates Workers if needed.
	-sends newPDFtasks to workers.
-donePDFtaskHandler - this thread initialized at Manager machine and run in infinity loop until system is down.
	-the thread receive donePDFtasks message from worker_to_manager queue.
	-insert the message to taskId_to_tasks hashtable, then check if all pdftasks related to taskId are done,
		if done, creates a doneTaskHandler thread that handle the rest Manager job.
	-delete the message from the queue and repeat.
-doneTaskHandler- when all PDFtasks of some taskId is ready this handler works as follow:
	-creates a summary file.
	-inserts all the donePDFtask that related to the specific taskId to the summary file line by line.
	-upload the summary file to s3.bucket.
	-sends a doneTask message by uniqe queue that related to a specific localApp.

these threads divide the 'Manager's work correctly and causes scalable programing.
	
once Manager get a termination message it get out ot the loop and wait until all tasks in process are done,
then, shut down the all system as mentioned.

notice that the Manager has an important role, if it dies all other localApp's newTask that waited in localApp_to_Manager queue should terminated by appropriate message,
until a new localApp is run and initializes another ec2 Manager instance.


The communication between localApp and Manager works as follow:
-newPDFtask message sent from Manager to Worker by one const queue that generated once when manager is initizalized aka 'Manager_to_worker' queue.
-donePDFtask message sent from Worker to Manager by one const queue that generated once when manager is initizalized aka 'worker_to_manager' queue.



Worker side:
Once Worker is initialized by Manager it workes in an infinite loop until being shut down by the manager upon system termination
first, Worker recieves messages, and change their visibility time.
second, download the pdf file- if fail, the Worker send appropriate donePDFtask message with description to Manager.
 otherwise, creates a new file from the original pdf as operation noted.
then, upload the new file to bucket and send donePDFtask to Manager.
when Worker finished with a message it delete it from the manager_to_worker queue.

in case that worker's instance died, the messages it handled retured to Manager_to_workers queue and will send to another worker (causes by visibility attribute).
 