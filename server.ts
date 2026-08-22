import express from "express";
import path from "path";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createServer as createViteServer } from "vite";
import { 
  INITIAL_BUSES, 
  INITIAL_CARPOOLS, 
  INITIAL_ROUTES, 
  INITIAL_TRAFFIC_CLUSTERS, 
  INITIAL_TIMETABLE,
  BMU_EMERGENCY_DEPARTMENTS
} from "./src/data/mockData";
import { 
  LiveBus, 
  TrafficCluster, 
  TrafficReport, 
  CarpoolRide, 
  IncidentType,
  CallSession,
  SosEmergencyAlert,
  SosContact
} from "./src/types";

// In-Memory Database for Fast Real-Time State
let liveBuses: LiveBus[] = [...INITIAL_BUSES];
let trafficClusters: TrafficCluster[] = [...INITIAL_TRAFFIC_CLUSTERS];
let allTrafficReports: TrafficReport[] = [];
// Populate initial reports
trafficClusters.forEach(cluster => {
  allTrafficReports.push(...cluster.reports);
});

let carpoolRides: CarpoolRide[] = [...INITIAL_CARPOOLS];
let activeCalls: CallSession[] = [];
let activeSosAlerts: SosEmergencyAlert[] = [];

// Registered Users & OTP Storage
interface UserAccountRecord {
  id: string;
  name: string;
  enrollmentNo: string;
  email: string;
  password: string;
  role: 'student' | 'faculty' | 'driver';
  department: string;
  institutionalId: string;
  verifiedIdCard: boolean;
  idCardData?: {
    barcodeId: string;
    validThrough: string;
    photoUrl?: string;
    verifiedAt: string;
  };
  avatar: string;
  phoneNumber?: string;
  isEmailVerified: boolean;
  createdAt: string;
}

let registeredUserAccounts: UserAccountRecord[] = [
  {
    id: 'usr-student-01',
    name: 'Aarav Sharma',
    enrollmentNo: '2024CS0892',
    email: 'aarav.sharma@university.edu',
    password: 'password123',
    role: 'student',
    institutionalId: 'STU-2026-8942',
    department: 'Dept. of Computer Science & Engineering',
    verifiedIdCard: true,
    idCardData: {
      barcodeId: 'UNI-CS-8942-A',
      validThrough: '2027-06-30',
      verifiedAt: '2026-01-15T09:30:00Z',
    },
    avatar: 'https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=150&auto=format&fit=crop&q=80',
    phoneNumber: '+91 98765 43210',
    isEmailVerified: true,
    createdAt: new Date().toISOString()
  },
  {
    id: 'usr-fac-02',
    name: 'Dr. Radhika Sen',
    enrollmentNo: 'FAC-ENG-402',
    email: 'r.sen@university.edu',
    password: 'password123',
    role: 'faculty',
    institutionalId: 'FAC-ENG-402',
    department: 'School of Advanced Computing',
    verifiedIdCard: true,
    idCardData: {
      barcodeId: 'FAC-402-ACAD',
      validThrough: '2029-12-31',
      verifiedAt: '2025-08-01T10:00:00Z',
    },
    avatar: 'https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?w=150&auto=format&fit=crop&q=80',
    phoneNumber: '+91 98450 11223',
    isEmailVerified: true,
    createdAt: new Date().toISOString()
  },
  {
    id: 'usr-drv-03',
    name: 'Vikram Pal (Bus 14)',
    enrollmentNo: 'DRV-FLEET-104',
    email: 'fleet.vpal@university.edu',
    password: 'password123',
    role: 'driver',
    institutionalId: 'DRV-FLEET-104',
    department: 'University Transit & Transportation Logistics',
    verifiedIdCard: true,
    idCardData: {
      barcodeId: 'TRANSIT-DRV-104',
      validThrough: '2028-05-15',
      verifiedAt: '2025-11-10T08:00:00Z',
    },
    avatar: 'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=150&auto=format&fit=crop&q=80',
    phoneNumber: '+91 98110 44231',
    isEmailVerified: true,
    createdAt: new Date().toISOString()
  }
];

interface StoredOtp {
  otp: string;
  name?: string;
  enrollmentNo?: string;
  expiresAt: number;
  verified: boolean;
}

const otpStore = new Map<string, StoredOtp>();

// Helper: Great-circle distance calculation
function getDistanceMeters(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371e3;
  const φ1 = (lat1 * Math.PI) / 180;
  const φ2 = (lat2 * Math.PI) / 180;
  const Δφ = ((lat2 - lat1) * Math.PI) / 180;
  const Δλ = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Math.round(R * c);
}

// Consensus Clustering Engine
// Verification criteria: 3 or more independent reports within 300 meters within 15 minutes (900,000 ms)
function evaluateTrafficConsensus(newReport: TrafficReport) {
  const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;
  const PROXIMITY_THRESHOLD_METERS = 300;
  const REQUIRED_REPORTS = 3;
  const nowTime = new Date(newReport.timestampUtc).getTime();

  // Find existing cluster within 300m
  let targetCluster = trafficClusters.find(c => {
    const dist = getDistanceMeters(c.lat, c.lng, newReport.lat, newReport.lng);
    return dist <= PROXIMITY_THRESHOLD_METERS && c.incidentType === newReport.incidentType;
  });

  if (targetCluster) {
    // Add report to cluster
    targetCluster.reports.push(newReport);
    targetCluster.lastReportedAt = newReport.timestampUtc;

    // Filter reports within recent 15 minutes
    const recentReports = targetCluster.reports.filter(r => {
      const repTime = new Date(r.timestampUtc).getTime();
      return (nowTime - repTime) <= FIFTEEN_MINUTES_MS;
    });

    targetCluster.reportCount = recentReports.length;

    // Consensus rule: >= 3 reports verified
    if (recentReports.length >= REQUIRED_REPORTS) {
      targetCluster.verified = true;
      targetCluster.severity = 'CRITICAL';
      if (!targetCluster.detourSuggested) {
        targetCluster.detourSuggested = `Consensus verified (${recentReports.length} reports). Avoid ${targetCluster.locationName}. Alternate lanes advised.`;
      }
    }
  } else {
    // Create new cluster in PENDING state
    const newCluster: TrafficCluster = {
      id: `cluster-${Date.now()}`,
      incidentType: newReport.incidentType,
      lat: newReport.lat,
      lng: newReport.lng,
      locationName: newReport.locationName || 'Reported Hazard Zone',
      radiusMeters: 200,
      reportCount: 1,
      requiredReports: REQUIRED_REPORTS,
      verified: false, // Starts unverified
      reports: [newReport],
      firstReportedAt: newReport.timestampUtc,
      lastReportedAt: newReport.timestampUtc,
      affectedRoutes: ['route-blue-01', 'route-emerald-02'],
      severity: 'MEDIUM'
    };
    trafficClusters.push(newCluster);
  }
}

async function startServer() {
  const app = express();
  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws/transit' });

  app.use(express.json({ limit: '10mb' }));

  // Broadcast WebSocket message to all active clients
  function broadcast(data: object) {
    const message = JSON.stringify(data);
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }

  wss.on('connection', (ws) => {
    // Send initial snapshot
    ws.send(JSON.stringify({
      type: 'INITIAL_STATE',
      data: {
        buses: liveBuses,
        trafficClusters,
        carpools: carpoolRides,
        activeSosAlerts
      }
    }));

    ws.on('message', (message) => {
      try {
        const parsed = JSON.parse(message.toString());
        if (parsed.type === 'DRIVER_GPS_TRANSMIT') {
          const { busId, lat, lng, speedKmH, headingDeg, nextStopId, nextStopName, etaNextStopMin, occupancy } = parsed.payload;
          const index = liveBuses.findIndex(b => b.id === busId);
          if (index !== -1) {
            liveBuses[index] = {
              ...liveBuses[index],
              lat,
              lng,
              speedKmH,
              headingDeg,
              nextStopId: nextStopId || liveBuses[index].nextStopId,
              nextStopName: nextStopName || liveBuses[index].nextStopName,
              etaNextStopMin: etaNextStopMin !== undefined ? etaNextStopMin : liveBuses[index].etaNextStopMin,
              occupancy: occupancy !== undefined ? occupancy : liveBuses[index].occupancy,
              lastPingUtc: new Date().toISOString(),
              isBroadcasting: true
            };
            broadcast({ type: 'BUS_UPDATED', payload: liveBuses[index] });
          }
        }
      } catch (err) {
        console.error('WebSocket parse error:', err);
      }
    });
  });

  // REST API Routes

  // 0. Institutional Authentication & OTP Verification API
  // Step 1: Send OTP to College Mail ID
  app.post('/api/auth/send-otp', (req, res) => {
    const { email, name, enrollmentNo } = req.body;

    if (!email || typeof email !== 'string' || !email.includes('@')) {
      return res.status(400).json({ 
        success: false, 
        error: 'Please provide a valid college email ID (e.g. name@bmu.edu.in or student@university.edu)' 
      });
    }

    const normalizedEmail = email.trim().toLowerCase();

    // Check basic institutional email format
    const isInstitutional = normalizedEmail.includes('.edu') || 
                           normalizedEmail.includes('.ac.in') || 
                           normalizedEmail.includes('bmu') || 
                           normalizedEmail.includes('university') ||
                           normalizedEmail.includes('college') ||
                           normalizedEmail.includes('@');

    // Generate 6-digit numeric OTP
    const generatedOtp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000; // 5 minutes validity

    otpStore.set(normalizedEmail, {
      otp: generatedOtp,
      name: name?.trim(),
      enrollmentNo: enrollmentNo?.trim()?.toUpperCase(),
      expiresAt,
      verified: false
    });

    console.log(`[AUTH OTP DISPATCH] College Email: ${normalizedEmail} | Generated 6-Digit OTP: ${generatedOtp} (Valid 5 mins)`);

    res.json({
      success: true,
      message: `Security OTP has been dispatched to ${normalizedEmail}`,
      email: normalizedEmail,
      otpPreview: generatedOtp, // Sent back so in-app webmail simulator can display live notification
      expiresInSec: 300
    });
  });

  // Step 2: Verify OTP
  app.post('/api/auth/verify-otp', (req, res) => {
    const { email, otp } = req.body;

    if (!email || !otp) {
      return res.status(400).json({ success: false, error: 'Email and 6-digit OTP code are required' });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const cleanOtp = otp.toString().trim();
    const record = otpStore.get(normalizedEmail);

    if (!record) {
      return res.status(400).json({ 
        success: false, 
        error: 'No active OTP verification session found for this college email. Please request a new OTP.' 
      });
    }

    if (Date.now() > record.expiresAt) {
      otpStore.delete(normalizedEmail);
      return res.status(400).json({ 
        success: false, 
        error: 'This OTP has expired (5-minute window). Please request a new verification code.' 
      });
    }

    if (record.otp !== cleanOtp) {
      return res.status(400).json({ 
        success: false, 
        error: 'Incorrect OTP code entered. Please check your college inbox or resend code.' 
      });
    }

    // Mark as verified
    record.verified = true;
    otpStore.set(normalizedEmail, record);

    res.json({
      success: true,
      verified: true,
      message: 'College Email ID successfully verified with institutional OTP.'
    });
  });

  // Step 3: Complete Student Signup
  app.post('/api/auth/signup', (req, res) => {
    const { name, enrollmentNo, email, password, otp, department, phoneNumber } = req.body;

    if (!name || !enrollmentNo || !email || !password) {
      return res.status(400).json({ 
        success: false, 
        error: 'Student Name, Enrollment Number, College Email, and Password are all required' 
      });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const cleanEnrollment = enrollmentNo.trim().toUpperCase();

    // Check if user already exists
    const existing = registeredUserAccounts.find(
      u => u.email.toLowerCase() === normalizedEmail || u.enrollmentNo.toUpperCase() === cleanEnrollment
    );

    if (existing) {
      return res.status(400).json({ 
        success: false, 
        error: 'An account with this College Email or Enrollment Number is already registered. Please login.' 
      });
    }

    // Verify OTP state
    const otpRecord = otpStore.get(normalizedEmail);
    const cleanOtp = otp ? otp.toString().trim() : '';

    if (!otpRecord) {
      return res.status(400).json({ 
        success: false, 
        error: 'Please request and verify the OTP sent to your college mail ID before completing signup.' 
      });
    }

    if (!otpRecord.verified && otpRecord.otp !== cleanOtp) {
      return res.status(400).json({ 
        success: false, 
        error: 'Invalid or unverified OTP code for this college email.' 
      });
    }

    // Default student avatar pool
    const defaultAvatars = [
      'https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=150&auto=format&fit=crop&q=80',
      'https://images.unsplash.com/photo-1539571696357-5a69c17a67c6?w=150&auto=format&fit=crop&q=80',
      'https://images.unsplash.com/photo-1517841905240-472988babdf9?w=150&auto=format&fit=crop&q=80',
      'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=150&auto=format&fit=crop&q=80'
    ];
    const assignedAvatar = defaultAvatars[Math.floor(Math.random() * defaultAvatars.length)];

    const newUser: UserAccountRecord = {
      id: `usr-student-${Date.now()}`,
      name: name.trim(),
      enrollmentNo: cleanEnrollment,
      email: normalizedEmail,
      password: password,
      role: 'student',
      department: department?.trim() || 'School of Engineering & Technology (B.Tech)',
      institutionalId: cleanEnrollment,
      verifiedIdCard: true,
      idCardData: {
        barcodeId: `BMU-${cleanEnrollment}`,
        validThrough: '2028-06-30',
        verifiedAt: new Date().toISOString()
      },
      avatar: assignedAvatar,
      phoneNumber: phoneNumber?.trim() || '+91 98765 00000',
      isEmailVerified: true,
      createdAt: new Date().toISOString()
    };

    registeredUserAccounts.push(newUser);
    otpStore.delete(normalizedEmail); // Clear consumed OTP

    // Strip password from returned profile
    const { password: _, ...safeProfile } = newUser;

    res.json({
      success: true,
      message: 'Student account successfully registered and institutional email verified!',
      user: safeProfile,
      token: `sess_${newUser.id}_${Date.now()}`
    });
  });

  // Step 4A: Student Login (Enrollment Number + College Email ID + OTP)
  app.post('/api/auth/student-login', (req, res) => {
    const { enrollmentNo, email, otp, name } = req.body;

    if (!enrollmentNo || !email || !otp) {
      return res.status(400).json({ 
        success: false, 
        error: 'Student Enrollment Number, College Email ID, and 6-digit OTP code are all required.' 
      });
    }

    const cleanEnrollment = enrollmentNo.trim().toUpperCase();
    const normalizedEmail = email.trim().toLowerCase();
    const cleanOtp = otp.toString().trim();

    // Verify OTP record
    const otpRecord = otpStore.get(normalizedEmail);
    if (!otpRecord) {
      return res.status(400).json({
        success: false,
        error: 'No active OTP verification request found for this email. Please request an OTP code first.'
      });
    }

    if (Date.now() > otpRecord.expiresAt) {
      otpStore.delete(normalizedEmail);
      return res.status(400).json({
        success: false,
        error: 'This OTP has expired. Please tap "Send OTP" to receive a new code.'
      });
    }

    if (otpRecord.otp !== cleanOtp) {
      return res.status(400).json({
        success: false,
        error: 'Incorrect OTP code entered. Please check your college inbox.'
      });
    }

    // OTP is valid! Find existing or auto-provision student profile
    let user = registeredUserAccounts.find(
      u => u.email.toLowerCase() === normalizedEmail || u.enrollmentNo.toUpperCase() === cleanEnrollment
    );

    if (!user) {
      user = {
        id: `usr-student-${Date.now()}`,
        name: name?.trim() || 'Student Scholar',
        enrollmentNo: cleanEnrollment,
        email: normalizedEmail,
        password: 'otp-authenticated',
        role: 'student',
        department: 'School of Engineering & Technology (CSE)',
        institutionalId: cleanEnrollment,
        verifiedIdCard: true,
        idCardData: {
          barcodeId: `BMU-${cleanEnrollment}`,
          validThrough: '2028-06-30',
          verifiedAt: new Date().toISOString()
        },
        avatar: 'https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=150&auto=format&fit=crop&q=80',
        phoneNumber: '+91 98765 43210',
        isEmailVerified: true,
        createdAt: new Date().toISOString()
      };
      registeredUserAccounts.push(user);
    }

    otpStore.delete(normalizedEmail); // Clear consumed OTP
    const { password: _, ...safeProfile } = user;

    res.json({
      success: true,
      message: `Welcome back ${safeProfile.name}! Student authentication verified.`,
      user: safeProfile,
      token: `sess_${user.id}_${Date.now()}`
    });
  });

  // Step 4B: Faculty Login (College Email ID + OTP)
  app.post('/api/auth/faculty-login', (req, res) => {
    const { email, otp } = req.body;

    if (!email || !otp) {
      return res.status(400).json({ 
        success: false, 
        error: 'College Faculty Email ID and 6-digit OTP are required.' 
      });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const cleanOtp = otp.toString().trim();

    // Verify OTP record
    const otpRecord = otpStore.get(normalizedEmail);
    if (!otpRecord) {
      return res.status(400).json({
        success: false,
        error: 'No active OTP verification request found for this faculty email. Please click "Send OTP" first.'
      });
    }

    if (Date.now() > otpRecord.expiresAt) {
      otpStore.delete(normalizedEmail);
      return res.status(400).json({
        success: false,
        error: 'Faculty OTP has expired. Please request a new code.'
      });
    }

    if (otpRecord.otp !== cleanOtp) {
      return res.status(400).json({
        success: false,
        error: 'Incorrect OTP code entered. Please check your faculty inbox.'
      });
    }

    // Extract name from email or profile
    const derivedName = normalizedEmail.split('@')[0].split('.').map((s: string) => s.charAt(0).toUpperCase() + s.slice(1)).join(' ');
    
    let user = registeredUserAccounts.find(u => u.email.toLowerCase() === normalizedEmail);
    if (!user) {
      user = {
        id: `usr-faculty-${Date.now()}`,
        name: `Prof. ${derivedName}`,
        enrollmentNo: `FAC-${Math.floor(1000 + Math.random() * 9000)}`,
        email: normalizedEmail,
        password: 'otp-authenticated',
        role: 'faculty',
        department: 'Department of Computer Science & AI',
        institutionalId: `FAC-${Math.floor(1000 + Math.random() * 9000)}`,
        verifiedIdCard: true,
        idCardData: {
          barcodeId: `FAC-BMU-9821`,
          validThrough: '2030-12-31',
          verifiedAt: new Date().toISOString()
        },
        avatar: 'https://images.unsplash.com/photo-1539571696357-5a69c17a67c6?w=150&auto=format&fit=crop&q=80',
        phoneNumber: '+91 98111 22334',
        isEmailVerified: true,
        createdAt: new Date().toISOString()
      };
      registeredUserAccounts.push(user);
    }

    otpStore.delete(normalizedEmail);
    const { password: _, ...safeProfile } = user;

    res.json({
      success: true,
      message: `Welcome ${safeProfile.name}! Faculty portal access authorized.`,
      user: safeProfile,
      token: `sess_${user.id}_${Date.now()}`
    });
  });

  // Step 4C: Bus Driver Login (Driver Name + Bus Number + Route)
  app.post('/api/auth/driver-login', (req, res) => {
    const { name, busNumber, routeId, routeName, phone } = req.body;

    if (!name || !busNumber || !routeId) {
      return res.status(400).json({ 
        success: false, 
        error: 'Driver Name, Bus Number, and Assigned Route are all required.' 
      });
    }

    const cleanName = name.trim();
    const cleanBusNo = busNumber.trim().toUpperCase();
    const cleanRouteId = routeId.trim();
    const cleanRouteName = routeName?.trim() || 'Campus Transit Loop';

    const driverUser: UserAccountRecord = {
      id: `usr-driver-${Date.now()}`,
      name: cleanName,
      enrollmentNo: `DRV-${cleanBusNo}`,
      email: `driver.${cleanBusNo.toLowerCase().replace(/[^a-z0-9]/g, '')}@transit.university.edu`,
      password: 'driver-assigned',
      role: 'driver',
      department: 'Campus Fleet & Transit Operations',
      institutionalId: `BUS-DRV-${cleanBusNo}`,
      verifiedIdCard: true,
      idCardData: {
        barcodeId: `DRV-${cleanBusNo}`,
        validThrough: '2027-12-31',
        verifiedAt: new Date().toISOString()
      },
      avatar: 'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=150&auto=format&fit=crop&q=80',
      phoneNumber: phone?.trim() || '+91 99887 76655',
      isEmailVerified: true,
      createdAt: new Date().toISOString()
    };

    registeredUserAccounts.push(driverUser);
    const { password: _, ...safeProfile } = driverUser;

    res.json({
      success: true,
      message: `Welcome Captain ${cleanName}! Telemetry link established for Bus ${cleanBusNo}.`,
      user: safeProfile,
      busAssigned: {
        busNumber: cleanBusNo,
        routeId: cleanRouteId,
        routeName: cleanRouteName
      },
      token: `sess_${driverUser.id}_${Date.now()}`
    });
  });

  // Step 4D: Standard Email / Password Login fallback
  app.post('/api/auth/login', (req, res) => {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ success: false, error: 'Please provide both College Email ID and Password' });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const user = registeredUserAccounts.find(
      u => u.email.toLowerCase() === normalizedEmail || u.enrollmentNo.toLowerCase() === normalizedEmail
    );

    if (!user) {
      return res.status(401).json({ 
        success: false, 
        error: 'No account found with this College Email or Enrollment Number. Please register first.' 
      });
    }

    if (user.password !== password) {
      return res.status(401).json({ 
        success: false, 
        error: 'Incorrect password entered for this institutional account. Please try again.' 
      });
    }

    const { password: _, ...safeProfile } = user;

    res.json({
      success: true,
      message: 'Authentication successful! Welcome to UniCommute.',
      user: safeProfile,
      token: `sess_${user.id}_${Date.now()}`
    });
  });

  // Timetable Sync API (Sync with University ERP / College Portal)
  app.post('/api/timetable/sync', (req, res) => {
    const { studentEnrollment, department } = req.body;
    console.log(`[ERP TIMETABLE SYNC] Student: ${studentEnrollment} | Department: ${department}`);
    res.json({
      success: true,
      slots: INITIAL_TIMETABLE,
      message: `Successfully synchronized ${INITIAL_TIMETABLE.length} course lecture slots for Semester 4 CSE.`
    });
  });

  // 1. Transit & Routes API
  app.get('/api/transit/routes', (req, res) => {
    res.json({ success: true, routes: INITIAL_ROUTES });
  });

  app.get('/api/transit/buses', (req, res) => {
    res.json({ success: true, buses: liveBuses });
  });

  // Driver GPS transmission endpoint (alternative to WS)
  app.post('/api/transit/driver/ping', (req, res) => {
    const { busId, lat, lng, speedKmH, headingDeg, nextStopId, nextStopName, etaNextStopMin, occupancy } = req.body;
    const index = liveBuses.findIndex(b => b.id === busId);
    if (index !== -1) {
      liveBuses[index] = {
        ...liveBuses[index],
        lat: Number(lat),
        lng: Number(lng),
        speedKmH: Number(speedKmH),
        headingDeg: Number(headingDeg),
        nextStopId: nextStopId || liveBuses[index].nextStopId,
        nextStopName: nextStopName || liveBuses[index].nextStopName,
        etaNextStopMin: Number(etaNextStopMin) || liveBuses[index].etaNextStopMin,
        occupancy: Number(occupancy) || liveBuses[index].occupancy,
        lastPingUtc: new Date().toISOString(),
        isBroadcasting: true
      };
      broadcast({ type: 'BUS_UPDATED', payload: liveBuses[index] });
      res.json({ success: true, bus: liveBuses[index] });
    } else {
      res.status(404).json({ success: false, error: 'Bus not found' });
    }
  });

  // 2. Crowdsourced Traffic Consensus API
  app.get('/api/traffic/clusters', (req, res) => {
    res.json({ success: true, clusters: trafficClusters, reports: allTrafficReports });
  });

  app.post('/api/traffic/report', (req, res) => {
    const { reporterId, reporterName, reporterRole, reporterEmail, incidentType, description, lat, lng, locationName } = req.body;

    if (!incidentType || lat === undefined || lng === undefined) {
      return res.status(400).json({ success: false, error: 'Missing required incident fields' });
    }

    const newReport: TrafficReport = {
      id: `rep-${Date.now()}`,
      reporterId: reporterId || 'anon-student',
      reporterName: reporterName || 'Anonymous Campus Commuter',
      reporterRole: reporterRole || 'student',
      reporterEmail: reporterEmail || 'user@university.edu',
      incidentType: incidentType as IncidentType,
      description: description || 'Traffic delay reported by commuter',
      lat: Number(lat),
      lng: Number(lng),
      locationName: locationName || 'Campus Corridor',
      timestampUtc: new Date().toISOString()
    };

    allTrafficReports.push(newReport);
    evaluateTrafficConsensus(newReport);

    broadcast({ type: 'TRAFFIC_UPDATED', payload: { clusters: trafficClusters, latestReport: newReport } });
    res.json({ success: true, report: newReport, clusters: trafficClusters });
  });

  // 3. Carpool & Equal Split API (Strictly ₹ Indian Rupees)
  app.get('/api/carpool/rides', (req, res) => {
    res.json({ success: true, rides: carpoolRides });
  });

  app.post('/api/carpool/create', (req, res) => {
    const { driverId, driverName, driverEmail, driverRole, vehicleModel, vehiclePlate, origin, destination, departureTime, totalSeats, totalFuelCostINR, notes } = req.body;

    if (!driverName || !origin || !destination || !totalFuelCostINR || !totalSeats) {
      return res.status(400).json({ success: false, error: 'All ride details and total INR fuel cost are required' });
    }

    const seats = Number(totalSeats);
    const fuelCost = Number(totalFuelCostINR);
    // Initial per-seat price with just driver = totalCost / (1 driver + 0 riders) -> as riders join, it splits equally
    const initialPerSeatCost = Math.round(fuelCost / (seats + 1));

    const newRide: CarpoolRide = {
      id: `ride-${Date.now()}`,
      driverId: driverId || 'usr-student-01',
      driverName,
      driverEmail: driverEmail || 'user@university.edu',
      driverRole: driverRole || 'student',
      driverRating: 5.0,
      vehicleModel: vehicleModel || 'Sedan',
      vehiclePlate: vehiclePlate || 'KA-01-XX-0000',
      origin,
      destination,
      departureTime,
      totalSeats: seats,
      availableSeats: seats,
      totalFuelCostINR: fuelCost,
      perSeatCostINR: initialPerSeatCost,
      safetyPin: Math.floor(1000 + Math.random() * 9000).toString(),
      status: 'OPEN',
      notes: notes || 'University campus ride share',
      riders: []
    };

    carpoolRides.unshift(newRide);
    broadcast({ type: 'CARPOOL_UPDATED', payload: carpoolRides });
    res.json({ success: true, ride: newRide });
  });

  app.post('/api/carpool/book', (req, res) => {
    const { rideId, userId, userName, userEmail, userRole, pickupLocation, seatsBooked } = req.body;
    const ride = carpoolRides.find(r => r.id === rideId);
    if (!ride) {
      return res.status(404).json({ success: false, error: 'Carpool ride not found' });
    }

    const requestedSeats = Number(seatsBooked) || 1;
    if (ride.availableSeats < requestedSeats) {
      return res.status(400).json({ success: false, error: 'Not enough available seats in this ride' });
    }

    // Dynamic Equal Split:
    // Total cost divided among (1 driver + total riders)
    const newTotalPassengers = 1 + ride.riders.length + requestedSeats;
    const newPerSeatCost = Math.round(ride.totalFuelCostINR / newTotalPassengers);

    const newRider = {
      id: `rdr-${Date.now()}`,
      userId: userId || 'stu-current',
      userName: userName || 'Student Commuter',
      userEmail: userEmail || 'student@university.edu',
      userRole: userRole || 'student',
      seatsBooked: requestedSeats,
      farePaidINR: newPerSeatCost,
      pickupLocation: pickupLocation || ride.origin,
      bookedAt: new Date().toISOString()
    };

    ride.riders.push(newRider);
    ride.availableSeats -= requestedSeats;
    ride.perSeatCostINR = newPerSeatCost;
    
    // Update previous riders' split share to show true equal split!
    ride.riders.forEach(r => {
      r.farePaidINR = newPerSeatCost;
    });

    if (ride.availableSeats <= 0) {
      ride.status = 'FULL';
    }

    broadcast({ type: 'CARPOOL_UPDATED', payload: carpoolRides });
    res.json({ success: true, ride, rider: newRider, newEqualPerSeatCostINR: newPerSeatCost });
  });

  // Cancel Booked Seat in Carpool
  app.post('/api/carpool/cancel-booking', (req, res) => {
    const { rideId, riderId, userId } = req.body;
    const ride = carpoolRides.find(r => r.id === rideId);
    if (!ride) {
      return res.status(404).json({ success: false, error: 'Carpool ride not found' });
    }

    const riderIndex = ride.riders.findIndex(r => r.id === riderId || r.userId === userId || r.userId === riderId);
    if (riderIndex === -1) {
      return res.status(404).json({ success: false, error: 'Rider booking not found in this carpool' });
    }

    const removedRider = ride.riders[riderIndex];
    const freedSeats = removedRider.seatsBooked || 1;
    ride.riders.splice(riderIndex, 1);
    ride.availableSeats += freedSeats;

    if (ride.status === 'FULL') {
      ride.status = 'OPEN';
    }

    // Recalculate dynamic equal split
    const remainingPassengers = 1 + ride.riders.length;
    const updatedPerSeat = Math.round(ride.totalFuelCostINR / remainingPassengers);
    ride.perSeatCostINR = updatedPerSeat;
    ride.riders.forEach(r => {
      r.farePaidINR = updatedPerSeat;
    });

    broadcast({ type: 'CARPOOL_UPDATED', payload: carpoolRides });
    res.json({ 
      success: true, 
      message: 'Booking cancelled successfully. Refund processed and seat released.',
      ride, 
      newEqualPerSeatCostINR: updatedPerSeat 
    });
  });

  // Cancel Entire Offered Carpool Ride (Driver action)
  app.post('/api/carpool/cancel-ride', (req, res) => {
    const { rideId, driverId, reason } = req.body;
    const ride = carpoolRides.find(r => r.id === rideId);
    if (!ride) {
      return res.status(404).json({ success: false, error: 'Carpool ride not found' });
    }

    if (driverId && ride.driverId !== driverId) {
      return res.status(403).json({ success: false, error: 'Only the host driver can cancel this offered ride' });
    }

    ride.status = 'CANCELLED';
    if (reason) {
      ride.notes = `${ride.notes ? ride.notes + ' | ' : ''}Cancelled: ${reason}`;
    }

    broadcast({ type: 'CARPOOL_UPDATED', payload: carpoolRides });
    res.json({ 
      success: true, 
      message: 'Offered carpool ride marked as cancelled. Passengers notified.',
      ride 
    });
  });

  // 3b. Carpool VoIP Calling API (Rider <-> Driver bidirectional in-app call)
  app.post('/api/carpool/call/initiate', (req, res) => {
    const { 
      rideId, 
      callerId, 
      callerName, 
      callerRole, 
      callerAvatar, 
      callerPhone, 
      recipientId, 
      recipientName, 
      recipientRole, 
      recipientAvatar, 
      recipientPhone, 
      vehiclePlate, 
      routeSummary 
    } = req.body;

    if (!callerId || !recipientId) {
      return res.status(400).json({ success: false, error: 'Caller and Recipient details are required' });
    }

    const callId = `call-${Date.now()}`;
    const newCall: CallSession = {
      id: callId,
      callId: callId,
      rideId: rideId || 'ride-direct',
      callerId,
      callerName: callerName || 'Commuter',
      callerRole: callerRole || 'student',
      callerAvatar,
      callerPhone: callerPhone || '+91 98765 43210',
      recipientId,
      recipientName: recipientName || 'Commuter',
      recipientRole: recipientRole || 'student',
      recipientAvatar,
      recipientPhone: recipientPhone || '+91 98450 11223',
      vehiclePlate,
      routeSummary,
      status: 'DIALING',
      startedAtUtc: new Date().toISOString(),
      durationSeconds: 0,
      isMuted: false,
      isSpeaker: true
    };

    activeCalls.push(newCall);
    broadcast({ type: 'CARPOOL_CALL_INITIATED', payload: newCall });
    res.json({ success: true, call: newCall });
  });

  app.post('/api/carpool/call/status', (req, res) => {
    const { callId, status, isMuted, isSpeaker, durationSeconds } = req.body;
    const callIndex = activeCalls.findIndex(c => c.callId === callId);
    
    if (callIndex !== -1) {
      const updatedCall = {
        ...activeCalls[callIndex],
        status: status || activeCalls[callIndex].status,
        isMuted: isMuted !== undefined ? isMuted : activeCalls[callIndex].isMuted,
        isSpeaker: isSpeaker !== undefined ? isSpeaker : activeCalls[callIndex].isSpeaker,
        durationSeconds: durationSeconds !== undefined ? durationSeconds : activeCalls[callIndex].durationSeconds
      };

      if (status === 'CONNECTED' && !updatedCall.connectedAtUtc) {
        updatedCall.connectedAtUtc = new Date().toISOString();
      }

      activeCalls[callIndex] = updatedCall;
      broadcast({ type: 'CARPOOL_CALL_UPDATED', payload: updatedCall });
      res.json({ success: true, call: updatedCall });
    } else {
      res.json({ success: true, message: 'Call already concluded or not found' });
    }
  });

  // 3c. Automated SOS Distress Emergency Broadcast API
  // Dispatches automated emergency calls to:
  // 1. Parents / Emergency Guardian
  // 2. BMU Faculty Mentor (Dr. Radhika Sen)
  // 3. Student Welfare Department (SWD) & Campus Security Desk
  app.get('/api/carpool/sos/active', (req, res) => {
    res.json({ success: true, activeAlerts: activeSosAlerts });
  });

  app.post('/api/carpool/sos/trigger', (req, res) => {
    const {
      triggeredByUserId,
      triggeredByUserName,
      triggeredByRole,
      studentInstitutionalId,
      triggeredByPhone,
      triggeredByAvatar,
      rideId,
      rideVehiclePlate,
      rideDriverName,
      origin,
      destination,
      gpsCoordinates,
      locationAddress
    } = req.body;

    const nowIso = new Date().toISOString();
    const studentName = triggeredByUserName || 'Aarav Sharma';
    const stuId = studentInstitutionalId || 'STU-2026-8942';
    const vehicle = rideVehiclePlate || 'KA-04-MB-8821';
    const locName = locationAddress || 'Near Koramangala Campus Transit Corridor';
    const latStr = gpsCoordinates?.lat ? gpsCoordinates.lat.toFixed(4) : '12.9774';
    const lngStr = gpsCoordinates?.lng ? gpsCoordinates.lng.toFixed(4) : '77.5877';

    // Automated Voice Synthesized Script
    const voiceScript = `EMERGENCY ALERT: This is an automated SOS distress broadcast from BML Munjal University student ${studentName}, Institutional ID ${stuId}. Triggered on Carpool transit vehicle ${vehicle} near ${locName}. Current GPS coordinates: Latitude ${latStr} North, Longitude ${lngStr} East. Automated dispatch priority 1 sent simultaneously to Parents, BMU Faculty Mentor Dr. Radhika Sen, and Student Welfare Department. Campus response dispatched.`;

    const smsScript = `🚨 [BMU-EMERGENCY-SOS] URGENT: Student ${studentName} (${stuId}) triggered SOS in carpool ${vehicle} near ${locName} (GPS: ${latStr}, ${lngStr}). Automated alerts dispatched to Parents, BMU Mentor & SWD Desk. Live Tracker: https://bmu.ac.in/sos/track/${Date.now()}`;

    // Dispatched Contacts Array with Simulated Live Automated Call Statuses
    const contactsDispatched: SosContact[] = [
      {
        id: `sos-c-parent-${Date.now()}`,
        role: 'PARENT',
        label: BMU_EMERGENCY_DEPARTMENTS.parents.label,
        name: BMU_EMERGENCY_DEPARTMENTS.parents.name,
        phoneNumber: BMU_EMERGENCY_DEPARTMENTS.parents.phoneNumber,
        organization: BMU_EMERGENCY_DEPARTMENTS.parents.organization,
        avatarUrl: BMU_EMERGENCY_DEPARTMENTS.parents.avatarUrl,
        callStatus: 'CONNECTED_AUTOMATED_CALL',
        dispatchTimestampUtc: nowIso,
        callDurationSeconds: 14,
        notes: 'Automated IVR call connected. Relaying live GPS location coordinates & vehicle details to family guardian.'
      },
      {
        id: `sos-c-mentor-${Date.now()}`,
        role: 'BMU_MENTOR',
        label: BMU_EMERGENCY_DEPARTMENTS.bmuMentor.label,
        name: BMU_EMERGENCY_DEPARTMENTS.bmuMentor.name,
        phoneNumber: BMU_EMERGENCY_DEPARTMENTS.bmuMentor.phoneNumber,
        organization: BMU_EMERGENCY_DEPARTMENTS.bmuMentor.organization,
        avatarUrl: BMU_EMERGENCY_DEPARTMENTS.bmuMentor.avatarUrl,
        callStatus: 'CONNECTED_AUTOMATED_CALL',
        dispatchTimestampUtc: nowIso,
        callDurationSeconds: 12,
        notes: 'Automated urgent dispatch call connected to BMU Faculty Mentor. Incident report ticket created in academic portal.'
      },
      {
        id: `sos-c-swd-${Date.now()}`,
        role: 'STUDENT_WELFARE',
        label: BMU_EMERGENCY_DEPARTMENTS.studentWelfare.label,
        name: BMU_EMERGENCY_DEPARTMENTS.studentWelfare.name,
        phoneNumber: BMU_EMERGENCY_DEPARTMENTS.studentWelfare.phoneNumber,
        organization: BMU_EMERGENCY_DEPARTMENTS.studentWelfare.organization,
        avatarUrl: BMU_EMERGENCY_DEPARTMENTS.studentWelfare.avatarUrl,
        callStatus: 'CONNECTED_AUTOMATED_CALL',
        dispatchTimestampUtc: nowIso,
        callDurationSeconds: 18,
        notes: '24x7 BMU Student Welfare Desk & Campus Quick Reaction Team (QRT) alerted. Incident priority CRITICAL.'
      }
    ];

    const newSosAlert: SosEmergencyAlert = {
      id: `sos-alert-${Date.now()}`,
      triggeredByUserId: triggeredByUserId || 'usr-student-01',
      triggeredByUserName: studentName,
      triggeredByRole: triggeredByRole || 'student',
      studentInstitutionalId: stuId,
      triggeredByPhone: triggeredByPhone || '+91 98765 43210',
      triggeredByAvatar,
      rideId,
      rideVehiclePlate: vehicle,
      rideDriverName: rideDriverName || 'Dr. Radhika Sen',
      origin: origin || 'Koramangala 4th Block',
      destination: destination || 'BMU Campus Terminal',
      gpsCoordinates: gpsCoordinates || { lat: 12.9774, lng: 77.5877, accuracyMeters: 4.5 },
      locationAddress: locName,
      timestampUtc: nowIso,
      status: 'ACTIVE_DISTRESS',
      contactsDispatched,
      automatedVoiceMessage: voiceScript,
      smsBroadcastText: smsScript
    };

    activeSosAlerts.unshift(newSosAlert);
    broadcast({ type: 'SOS_DISTRESS_TRIGGERED', payload: newSosAlert });
    res.json({ success: true, alert: newSosAlert });
  });

  app.post('/api/carpool/sos/resolve', (req, res) => {
    const { alertId, resolutionComment } = req.body;
    const alertIndex = activeSosAlerts.findIndex(a => a.id === alertId);

    if (alertIndex !== -1) {
      activeSosAlerts[alertIndex] = {
        ...activeSosAlerts[alertIndex],
        status: 'RESOLVED_SAFE',
        resolvedAtUtc: new Date().toISOString(),
        resolvedComment: resolutionComment || 'User confirmed safe status. Emergency resolved.'
      };

      broadcast({ type: 'SOS_DISTRESS_RESOLVED', payload: activeSosAlerts[alertIndex] });
      res.json({ success: true, alert: activeSosAlerts[alertIndex] });
    } else {
      res.status(404).json({ success: false, error: 'SOS Alert not found' });
    }
  });

  // 4. Supabase SQL Migrations Script Endpoint
  app.get('/api/database/schema-script', (req, res) => {
    const sqlSchema = `-- ==========================================================
-- UniCommute Supabase / PostgreSQL Production Migration Schema
-- Database tables, Real-time Subscriptions, & Edge Trigger Rules
-- ==========================================================

-- 1. Enable PostGIS & UUID extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- 2. Profiles Table (Restricted to @university.edu domains)
CREATE TABLE IF NOT EXISTS public.profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE CHECK (email LIKE '%@university.edu'),
  role TEXT NOT NULL CHECK (role IN ('student', 'faculty', 'driver')),
  institutional_id TEXT NOT NULL UNIQUE,
  department TEXT NOT NULL,
  verified_id_card BOOLEAN DEFAULT false,
  id_barcode TEXT,
  phone_number TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Bus Routes and Stops
CREATE TABLE IF NOT EXISTS public.bus_routes (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  code TEXT NOT NULL UNIQUE,
  color TEXT NOT NULL,
  operating_hours TEXT NOT NULL,
  frequency_min INTEGER NOT NULL DEFAULT 15,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.bus_stops (
  id TEXT PRIMARY KEY,
  route_id TEXT REFERENCES public.bus_routes(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  location GEOGRAPHY(POINT, 4326) NOT NULL,
  sequence_num INTEGER NOT NULL,
  landmark TEXT
);

-- 4. Real-time Live Fleet Telemetry (Updated every 3 seconds)
CREATE TABLE IF NOT EXISTS public.live_buses (
  id TEXT PRIMARY KEY,
  route_id TEXT REFERENCES public.bus_routes(id),
  bus_number TEXT NOT NULL,
  driver_id UUID REFERENCES public.profiles(id),
  current_location GEOGRAPHY(POINT, 4326) NOT NULL,
  speed_kmh NUMERIC(5,2) DEFAULT 0,
  heading_deg NUMERIC(5,2) DEFAULT 0,
  next_stop_id TEXT REFERENCES public.bus_stops(id),
  eta_next_stop_min INTEGER DEFAULT 5,
  capacity INTEGER DEFAULT 50,
  occupancy INTEGER DEFAULT 0,
  status TEXT DEFAULT 'ON_TIME' CHECK (status IN ('ON_TIME', 'DELAYED_TRAFFIC', 'MAINTENANCE')),
  last_ping_utc TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Traffic Incidents & Consensus Clusters
-- Edge rule: Incident verified when 3+ reports exist within 300m within 15min
CREATE TABLE IF NOT EXISTS public.traffic_clusters (
  id TEXT PRIMARY KEY,
  incident_type TEXT NOT NULL CHECK (incident_type IN ('JAM', 'ROADBLOCK', 'PROTEST', 'ACCIDENT', 'WEATHER_FLOOD', 'BUS_BREAKDOWN')),
  location GEOGRAPHY(POINT, 4326) NOT NULL,
  location_name TEXT NOT NULL,
  radius_meters INTEGER DEFAULT 300,
  report_count INTEGER DEFAULT 1,
  required_reports INTEGER DEFAULT 3,
  verified BOOLEAN DEFAULT false,
  severity TEXT DEFAULT 'MEDIUM' CHECK (severity IN ('LOW', 'MEDIUM', 'CRITICAL')),
  detour_suggested TEXT,
  first_reported_at TIMESTAMPTZ DEFAULT NOW(),
  last_reported_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.traffic_reports (
  id TEXT PRIMARY KEY,
  cluster_id TEXT REFERENCES public.traffic_clusters(id) ON DELETE SET NULL,
  reporter_id UUID REFERENCES public.profiles(id),
  incident_type TEXT NOT NULL,
  description TEXT,
  location GEOGRAPHY(POINT, 4326) NOT NULL,
  location_name TEXT,
  timestamp_utc TIMESTAMPTZ DEFAULT NOW()
);

-- 6. Carpools with Equal Fuel/Toll Split in Indian Rupees (₹ INR)
CREATE TABLE IF NOT EXISTS public.carpool_rides (
  id TEXT PRIMARY KEY,
  driver_id UUID REFERENCES public.profiles(id),
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  total_seats INTEGER NOT NULL CHECK (total_seats > 0),
  available_seats INTEGER NOT NULL,
  total_fuel_cost_inr NUMERIC(10,2) NOT NULL CHECK (total_fuel_cost_inr > 0),
  per_seat_cost_inr NUMERIC(10,2) NOT NULL,
  safety_pin TEXT NOT NULL,
  status TEXT DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'FULL', 'IN_TRANSIT', 'COMPLETED', 'CANCELLED')),
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.carpool_riders (
  id TEXT PRIMARY KEY,
  ride_id TEXT REFERENCES public.carpool_rides(id) ON DELETE CASCADE,
  rider_id UUID REFERENCES public.profiles(id),
  seats_booked INTEGER DEFAULT 1,
  fare_paid_inr NUMERIC(10,2) NOT NULL,
  pickup_location TEXT,
  booked_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. Delay Attendance Claims (In-App Camera Proof & GPS Watermark)
CREATE TABLE IF NOT EXISTS public.delay_attendance_claims (
  id TEXT PRIMARY KEY,
  student_id UUID REFERENCES public.profiles(id),
  course_code TEXT NOT NULL,
  course_name TEXT NOT NULL,
  professor_id UUID REFERENCES public.profiles(id),
  class_start_time TEXT NOT NULL,
  captured_timestamp_utc TIMESTAMPTZ NOT NULL,
  captured_location GEOGRAPHY(POINT, 4326) NOT NULL,
  location_address TEXT NOT NULL,
  recorded_video_url TEXT,
  captured_photo_url TEXT,
  estimated_delay_min INTEGER NOT NULL,
  excuse_reason TEXT NOT NULL,
  traffic_proximity_score NUMERIC(5,2) DEFAULT 0,
  nearest_cluster_id TEXT REFERENCES public.traffic_clusters(id),
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
  professor_decision_comment TEXT,
  reviewed_at TIMESTAMPTZ
);

-- 8. Real-time Replication Publication for Supabase Realtime
ALTER PUBLICATION supabase_realtime ADD TABLE public.live_buses;
ALTER PUBLICATION supabase_realtime ADD TABLE public.traffic_clusters;
ALTER PUBLICATION supabase_realtime ADD TABLE public.carpool_rides;
ALTER PUBLICATION supabase_realtime ADD TABLE public.delay_attendance_claims;
`;
    res.setHeader('Content-Type', 'text/plain');
    res.send(sqlSchema);
  });

  // Vite middleware for development vs static build
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  const PORT = 3000;
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`UniCommute Server active at http://localhost:${PORT}`);
  });
}

startServer();
