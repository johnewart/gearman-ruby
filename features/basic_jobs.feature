Feature: Running basic jobs
    
    As a responsible developer
    I want to test my gearman-ruby library
    And make sure it works
    
    Scenario: A basic sleep job
        Given gearmand is running
        And a basic worker is running
        When I add a basic client
        Then the job should be processed
        And I should see "true"