# change this to apache/superset:5.0.0 or whatever version you want to build from;
# otherwise the default is the latest commit on GitHub master branch
FROM apache/superset:5.0.0

USER root

# Set environment variable for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers

# Install packages using uv into the virtual environment
# Superset started using uv after the 4.1 branch; if you are building from apache/superset:4.1.x or an older version,
# replace the first two lines with RUN pip install \
RUN . /app/.venv/bin/activate && \
    uv pip install \
    # install psycopg2 for using PostgreSQL metadata store - could be a MySQL package if using that backend:
    psycopg2-binary \
    # add the driver(s) for your data warehouse(s), in this example we're showing for Microsoft SQL Server:
    pymssql \
    # package needed for using single-sign on authentication:
    Authlib \
    # openpyxl to be able to upload Excel files
    openpyxl \
    # Pillow for Alerts & Reports to generate PDFs of dashboards
    Pillow \
    # install Playwright for taking screenshots for Alerts & Reports. This assumes the feature flag PLAYWRIGHT_REPORTS_AND_THUMBNAILS is enabled
    # That feature flag will default to True starting in 6.0.0
    # Playwright works only with Chrome.
    # If you are still using Selenium instead of Playwright, you would instead install here the selenium package and a headless browser & webdriver
    playwright \
    trino \
    && playwright install-deps \
    && PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers playwright install chromium

# Switch back to the superset user
USER superset

CMD ["/app/docker/entrypoints/run-server.sh"]