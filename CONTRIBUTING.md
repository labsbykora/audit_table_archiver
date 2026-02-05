# Contributing to Audit Table Archiver

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/auditlog_manager.git
   cd auditlog_manager
   ```

2. **Install Dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

3. **Set Up Pre-commit Hooks**
   ```bash
   pre-commit install
   ```

4. **Start Local Services**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

## Development Workflow

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**
   - Follow PEP 8 style guidelines
   - Add type hints to all functions
   - Write docstrings for public functions/classes
   - Add unit tests for new functionality

3. **Run Tests**
   ```bash
   # Unit tests
   pytest tests/unit/ -v

   # Integration tests (requires Docker)
   pytest tests/integration/ -v -m integration

   # All tests with coverage
   pytest --cov=archiver --cov-report=term-missing
   ```

4. **Check Code Quality**
   ```bash
   # Format code
   black .

   # Lint
   ruff check .

   # Type check
   mypy src/
   ```

5. **Commit Changes**
   - Use conventional commits format: `type(scope): description`
   - Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`
   - Example: `feat(archiver): add watermark tracking`

6. **Push and Create Pull Request**
   - Push to your fork
   - Create PR with clear description
   - Link to related issues

## Code Standards

### Python Style
- Follow PEP 8
- Use `black` for formatting (line length: 100)
- Use `ruff` for linting
- Use type hints throughout

### Testing
- Write unit tests for all new functionality
- Maintain >70% code coverage
- Use descriptive test names
- Test both success and failure paths

### Documentation
- Add docstrings to all public functions/classes
- Update README if adding features
- Update CHANGELOG for user-facing changes

## Pull Request Process

1. **Update Documentation**
   - Update README if needed
   - Update CHANGELOG for user-facing changes
   - Update docstrings

2. **Ensure Tests Pass**
   - All unit tests pass
   - Integration tests pass (if applicable)
   - Coverage maintained

3. **Code Review**
   - Address review comments
   - Ensure CI checks pass

4. **Merge**
   - Squash commits for cleaner history
   - Delete feature branch after merge

## Reporting Issues

When reporting issues, please include:
- Archiver version (`python -m archiver.main --version`)
- PostgreSQL version
- Configuration file (sanitized, no credentials)
- Error message and stack trace
- Steps to reproduce
- Expected vs actual behavior

## Feature Requests

For feature requests:
- Check if feature is already planned
- Open an issue with clear description
- Explain use case and benefits
- Be patient - we prioritize based on impact

## Questions?

- Open a GitHub Discussion for questions
- Check existing issues and discussions first
- Be respectful and constructive

Thank you for contributing! 🎉

