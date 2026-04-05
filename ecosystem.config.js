module.exports = {
  apps: [
    {
      name: 'ewaste-backend',
      script: 'app.js',
      cwd: './backend',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PORT: 5000
      },
      error_file: './logs/backend-error.log',
      out_file: './logs/backend-out.log',
      log_file: './logs/backend-combined.log',
      time: true
    },
    {
      name: 'ewaste-ml',
      script: 'gunicorn',
      cwd: './ml-service',
      args: '-w 4 -b 0.0.0.0:5001 app:app',
      interpreter: 'python3',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/ml-error.log',
      out_file: './logs/ml-out.log',
      time: true
    }
  ]
};
