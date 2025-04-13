# Deploying the FX Trading Bridge on Render

This guide explains how to deploy the FX Trading Bridge application on Render.

## Setup Repository

1. Push your code to a GitHub repository
2. Make sure all required files are present:
   - `enhanced_trading.py` - Main application
   - `requirements.txt` - Dependencies list
   - `render.yaml` - Render configuration
   - `Procfile` - Process definition (optional if using render.yaml)
   - `runtime.txt` - Python version specification
   - `.env.render` - Default environment variables

## Deploy on Render

1. Log in to your Render account
2. Click "New" and select "Blueprint"
3. Connect your GitHub repository
4. Select the repository with your FX Trading Bridge code
5. Render will detect the `render.yaml` and configure the services
6. Set the required environment variables:
   - `OANDA_ACCOUNT_ID`
   - `OANDA_API_TOKEN`
   - `JWT_SECRET`
7. Click "Apply" to deploy the application

## Accessing the Application

Once deployed, you can access:

- API endpoints at `https://your-app-name.onrender.com/api/`
- API documentation at `https://your-app-name.onrender.com/docs`
- Backtest dashboard at `https://your-app-name.onrender.com/dashboard`

## Visualizing Backtest Results

To use the visualization dashboard:

1. Run a backtest using the `/api/backtest/run` endpoint
2. Access the dashboard at `/dashboard`
3. Select your backtest from the dropdown
4. Explore the equity curve, performance metrics, and trade analysis

## Troubleshooting

If you encounter issues during deployment:

1. Check the Render logs for error messages
2. Verify that all dependencies are correctly specified in `requirements.txt`
3. Make sure the `fx` module and its submodules are correctly set up
4. Check that the environment variables are properly configured

## Updating the Application

To update your application:

1. Push changes to your GitHub repository
2. Render will automatically redeploy the application

## Managing Your Deployment

### Scaling

- To scale your app, go to your service settings and adjust:
  - **Instances**: Number of running instances
  - **Plan**: To upgrade hardware resources

### Logs

- Access logs via the **Logs** tab on your service page
- You can filter logs and search for specific events

### Monitoring

- Render provides basic metrics under the **Metrics** tab
- For more advanced monitoring, consider setting up a custom solution:
  1. Configure an external monitoring service (DataDog, New Relic, etc.)
  2. Add the monitoring agent to your Dockerfile
  3. Set required environment variables in Render

### Data Persistence

Render's disks are ephemeral. For persistent data:

1. Use Render's **Disk** add-on for your service
2. Update your code to store data in the disk mount path
3. For more critical data, consider using a managed database service

## Security Considerations

- Always use environment secrets for sensitive information
- Consider setting up IP restrictions in the Render dashboard
- Enable HTTPS (provided by default with Render)
- Implement authentication for your API endpoints
- Regularly review access logs for suspicious activity 