#Steps followed to register flow in prefect cloud account

1.  Connect local machine with cloud:-
    prefect backend cloud
 
2. Create an api key in prefect cloud account and logged with local machine:-
   prefect auth login --key your-key
  
3. Create a project in your cloud account either through the UI or by typing in the terminal:-
   prefect create project project_name
  
4. Run your flow in VS code:-
   python flow.py 
  
5.  At the top of the flow page, click Quick Run to run your flow.
  
6.  Start an agent by typing the following into your terminal:-prefect agent local start
  
  
 