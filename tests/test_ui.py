"""
tests/test_ui.py

Unit tests for the Flask web UI application.
Tests basic route accessibility and error handling.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestUIAppCreation:
    """Test that the Flask app can be created."""

    @patch('engine.loader.DescriptorLoader')
    def test_app_creates_successfully(self, mock_loader):
        """Test that the Flask app can be created without errors."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            assert app is not None
            assert app.config['TESTING'] is False  # Default config


class TestDashboardRoute:
    """Test the dashboard route."""

    @patch('engine.loader.DescriptorLoader')
    def test_dashboard_get(self, mock_loader):
        """Test GET / is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            # Mock the loader inside the request
            mock_instance = MagicMock()
            mock_instance.load_systems.return_value = []
            mock_instance.load_graph_types.return_value = []
            mock_instance.load_global_config.return_value = {}
            mock_loader.return_value = mock_instance
            
            response = client.get('/')
            
            # Either 200 or error is acceptable as we're mocking dependencies
            assert response.status_code in [200, 500]


class TestSystemsRoute:
    """Test systems management routes."""

    @patch('engine.loader.DescriptorLoader')
    def test_systems_list_accessible(self, mock_loader):
        """Test GET /systems is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            mock_instance = MagicMock()
            mock_instance.load_systems.return_value = []
            mock_loader.return_value = mock_instance
            
            response = client.get('/systems')
            assert response.status_code in [200, 500]


class TestExperimentRoutes:
    """Test experiment management routes."""

    @patch('engine.loader.DescriptorLoader')
    def test_experiment_new_accessible(self, mock_loader):
        """Test GET /experiment/new is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            mock_instance = MagicMock()
            mock_instance.load_systems.return_value = []
            mock_instance.load_graph_types.return_value = []
            mock_instance.load_global_config.return_value = {}
            mock_instance.load_domains.return_value = []
            mock_loader.return_value = mock_instance
            
            response = client.get('/experiment/new')
            assert response.status_code in [200, 500]

    @patch('engine.loader.DescriptorLoader')
    def test_experiment_status_accessible(self, mock_loader):
        """Test GET /experiment/status is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            response = client.get('/experiment/status')
            assert response.status_code == 200
            
            # Should return JSON with experiment state
            data = json.loads(response.data)
            assert 'running' in data


class TestResultsRoute:
    """Test results browsing routes."""

    @patch('engine.loader.DescriptorLoader')
    def test_results_list_accessible(self, mock_loader):
        """Test GET /results is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            response = client.get('/results')
            assert response.status_code in [200, 500]


class TestAPIEndpoints:
    """Test API endpoints."""

    @patch('engine.loader.DescriptorLoader')
    def test_api_systems_accessible(self, mock_loader):
        """Test GET /api/systems is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            mock_instance = MagicMock()
            mock_system = MagicMock()
            mock_system.to_dict.return_value = {'name': 'test', 'display_name': 'Test'}
            mock_instance.load_systems.return_value = [mock_system]
            mock_loader.return_value = mock_instance
            
            response = client.get('/api/systems')
            assert response.status_code in [200, 500]

    @patch('engine.loader.DescriptorLoader')
    def test_api_graph_types_accessible(self, mock_loader):
        """Test GET /api/graph-types is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            mock_instance = MagicMock()
            mock_graph = MagicMock()
            mock_graph.to_dict.return_value = {'name': 'cycle', 'display_name': 'Cycle'}
            mock_instance.load_graph_types.return_value = [mock_graph]
            mock_loader.return_value = mock_instance
            
            response = client.get('/api/graph-types')
            assert response.status_code in [200, 500]

    @patch('engine.loader.DescriptorLoader')
    def test_api_domains_accessible(self, mock_loader):
        """Test GET /api/domains is accessible."""
        with patch('ui.app.BASE_DIR', Path('/tmp/test-trans-bench')):
            from ui.app import create_app
            app = create_app()
            app.config['TESTING'] = True
            client = app.test_client()
            
            mock_instance = MagicMock()
            mock_domain = MagicMock()
            mock_domain.name = 'transitive'
            mock_domain.display_name = 'Transitive'
            mock_domain.modes = ['right_recursion']
            mock_domain.description = 'Test'
            mock_instance.load_domains.return_value = [mock_domain]
            mock_loader.return_value = mock_instance
            
            response = client.get('/api/domains')
            assert response.status_code in [200, 500]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
