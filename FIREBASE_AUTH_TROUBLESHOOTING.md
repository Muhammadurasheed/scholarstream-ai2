# Firebase Authentication Troubleshooting Guide

## Current Issue: "auth/invalid-credential"

### Root Cause Analysis

The error `auth/invalid-credential` from Firebase means one of the following:

1. **Wrong Password**: The password entered doesn't match the one stored in Firebase
2. **User Doesn't Exist**: The email is not registered in Firebase Auth
3. **Email/Password Provider Disabled**: Firebase Auth email/password sign-in is not enabled

### Diagnostic Steps

#### Step 1: Verify Firebase Auth is Enabled

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select project: `scholarstream-i4i`
3. Navigate to **Authentication** → **Sign-in method**
4. Verify these providers are **ENABLED**:
   - ✅ **Email/Password** (should be enabled)
   - ✅ **Google** (should be enabled)

#### Step 2: Check if User Exists

1. In Firebase Console → **Authentication** → **Users**
2. Search for email: `yekinirasheed2002@gmail.com`
3. If user doesn't exist:
   - User needs to sign up first
   - OR manually add user in Firebase Console

#### Step 3: Verify Password

- If user exists, try resetting password
- Use "Forgot Password" flow
- OR manually reset in Firebase Console

### Quick Fixes

#### Fix 1: Enable Email/Password Authentication

```bash
# In Firebase Console:
1. Go to Authentication → Sign-in method
2. Click "Email/Password"
3. Enable both toggles:
   - Email/Password: ENABLED
   - Email link (passwordless sign-in): ENABLED (optional)
4. Click "Save"
```

#### Fix 2: Enable Google Sign-In

```bash
# In Firebase Console:
1. Go to Authentication → Sign-in method
2. Click "Google"
3. Enable the toggle
4. Set support email (your email)
5. Click "Save"
```

#### Fix 3: Create Test User

```bash
# In Firebase Console:
1. Go to Authentication → Users
2. Click "Add user"
3. Enter:
   - Email: yekinirasheed2002@gmail.com
   - Password: (set a test password)
4. Click "Add user"
```

### Code Improvements Made

1. **Better Error Messages**: Updated AuthContext to show user-friendly errors
2. **Google Sign-In Fix**: Added proper error handling for Google auth
3. **Firestore Error Handling**: Made Firestore writes non-blocking

### Testing

After enabling authentication providers:

```bash
# Test Email/Password Sign-In
1. Go to http://localhost:8080/login
2. Enter email: yekinirasheed2002@gmail.com
3. Enter correct password
4. Should redirect to /dashboard or /onboarding

# Test Google Sign-In
1. Go to http://localhost:8080/login
2. Click "Sign in with Google"
3. Select Google account
4. Should redirect to /dashboard or /onboarding
```

### Common Errors

| Error Code | Meaning | Solution |
|------------|---------|----------|
| `auth/invalid-credential` | Wrong password or user doesn't exist | Check password, verify user exists |
| `auth/user-not-found` | Email not registered | Sign up first |
| `auth/wrong-password` | Incorrect password | Reset password |
| `auth/too-many-requests` | Too many failed attempts | Wait 15 minutes |
| `auth/popup-blocked` | Browser blocked Google popup | Allow popups for localhost |
| `auth/popup-closed-by-user` | User closed Google popup | Try again |

### Next Steps

1. **Enable Authentication Providers** in Firebase Console
2. **Create Test User** if needed
3. **Test Login** with correct credentials
4. **Test Google Sign-In**

**Allahu Musta'an** - Authentication should work after enabling providers!
